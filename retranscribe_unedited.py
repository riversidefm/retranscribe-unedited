from dataclasses import dataclass
from datetime import datetime
import pickle
import time
from typing import List, Optional
from pydantic import Field, BaseSettings
import requests
from snowflake.snowpark import Session
from sqlalchemy import text
from logging import Logger

from tqdm import tqdm

logger = Logger(__name__)


class GetRelevantSessionsConfig(BaseSettings):
    sf_account: str
    sf_user: str
    sf_password: str
    sf_role: str = "REPORTER"
    sf_warehouse: str = "COMPUTE_WH"
    sf_database: str = "SEGMENT_EVENTS"
    sf_schema: str = "WEB"
    start_date: datetime = Field(default_factory=lambda: datetime.now())
    already_completed_sessions: List[str] = Field(default_factory=list)


class FilterEditedSessionsConfig(BaseSettings):
    rds_host: str
    rds_port: int
    rds_database: str
    rds_user: str
    rds_password: str
    rds_sentences_table_fqdn: str = "transcriptions.sentences"


class RetranscribeConfig(BaseSettings):
    api_url: str = "https://riverside.fm/api/2/recordings/transcript"
    wait_between_requests_secs: float = 0.5


@dataclass
class State:
    already_completed_sessions: List[str]
    last_sealed_date: datetime


@dataclass
class StateHandler:
    state_pkl_path: str

    def load(self) -> Optional[State]:
        try:
            with open(self.state_pkl_path, "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return None

    def save(self, state: State) -> None:
        with open(self.state_pkl_path, "wb") as f:
            pickle.dump(state, f)


class StateConfig(BaseSettings):
    state_pkl_path: str = "state.pkl"
    load_state: bool = True
    save_state: bool = True


def _connect_to_mysql(host, port, database, user, password):
    """
    Connect to PostgreSQL database and return connection object.
    """
    from sqlalchemy import create_engine

    db_url = f"mysql://{user}:{password}@{host}:{port}/{database}"

    try:
        # Create an engine to connect to the database
        engine = create_engine(db_url)

        # Connect to the database
        connection = engine.connect()

        return connection

    except Exception as e:
        print("Something went wrong:", e)
        raise


def _fetch_data_by_session_id(connection, table_name, session_ids):
    """
    Fetch data from PostgreSQL table based on session_id.
    """
    if len(session_ids) == 0:
        return []
    query = f"""SELECT distinct(session_id) 
    FROM {table_name} 
    WHERE session_id in ({','.join(map(lambda s: f"'{s}'", session_ids))}) and (edited_words_v2 is not NULL) group by session_id"""
    result = connection.execute(text(query))
    sessions = [row[0] for row in result]
    return sessions


def _get_relevant_sessions_from_snowflake(
    config: GetRelevantSessionsConfig,
) -> List[str]:
    connection_parameters = {
        "account": config.sf_account,
        "user": config.sf_user,
        "password": config.sf_password,
        "role": config.sf_role,
        "warehouse": config.sf_warehouse,
        "database": config.sf_database,
        "schema": config.sf_schema,
    }
    session = Session.builder.configs(connection_parameters).create()
    sf_query_template = _get_sf_query(config.start_date)
    sf_rows = session.sql(sf_query_template).collect()
    session_ids = [
        row["SESSION_ID"]
        for row in sf_rows
        if row["SESSION_ID"] not in set(config.already_completed_sessions)
    ]

    return session_ids


def _filter_unedited_sessions_using_mysql(
    sessions: List[str], config: FilterEditedSessionsConfig
) -> List[str]:
    mysql_conn = _connect_to_mysql(
        config.rds_host,
        config.rds_port,
        config.rds_database,
        config.rds_user,
        config.rds_password,
    )
    edited_sessions = _fetch_data_by_session_id(
        mysql_conn, config.rds_sentences_table_fqdn, sessions
    )
    mysql_conn.close()
    return list(filter(lambda session: session not in edited_sessions, sessions))


def _build_retranscribe_url(api_url: str, session_id: str) -> str:
    return f"{api_url}/{session_id}"


def _run_retranscribe(session_ids: List[str], config: RetranscribeConfig) -> None:
    for session_id in tqdm(session_ids):
        url = _build_retranscribe_url(config.api_url, session_id)
        resp = requests.get(url)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            logger.error(f"Failed to retranscribe session {session_id}: {e}")
            raise
        logger.info(f"Retranscribed session {session_id}")
        time.sleep(config.wait_between_requests_secs)


def _get_sf_query(date: datetime):
    date_str = date.strftime("%Y-%m-%d %H:%M:%S")
    sf_query_template = """
    SELECT 
        r.session_id, 
        BOOLAND_AGG(r.status = 'done') AS all_done,
        BOOLOR_AGG(r.speaker_name ILIKE ('%screenshare%') OR r.source ILIKE ('%screenshare%')) AS has_screen_share,
        MIN(CASE WHEN studio_role = 'host' THEN r.server_timestamp END) AS host_server_timestamp,
        MIN(CASE WHEN r.speaker_name ILIKE ('%screenshare%') OR r.source ILIKE ('%screenshare%') THEN r.server_timestamp END) AS screen_share_server_timestamp
    FROM analytics.stg.stg_mongo__recordings r
    WHERE r.file_type NOT IN ('composed','composer')
    GROUP BY 1
    HAVING 
        all_done AND 
        has_screen_share 
        AND screen_share_server_timestamp IS NOT NULL 
        AND host_server_timestamp IS NOT NULL
        AND DATEDIFF('second',screen_share_server_timestamp,host_server_timestamp) >= 3
        AND TO_DATE(host_server_timestamp) >= '{date}'
    """

    return sf_query_template.format(date=date_str)


def main() -> None:
    now = datetime.now()
    state_config = StateConfig()
    state_handler = StateHandler(state_pkl_path=state_config.state_pkl_path)
    get_relevant_sessions_config = GetRelevantSessionsConfig()
    if state_config.load_state:
        state = state_handler.load()
        if state is not None:
            get_relevant_sessions_config.already_completed_sessions = (
                state.already_completed_sessions
            )
            get_relevant_sessions_config.start_date = state.last_sealed_date
    sessions = _get_relevant_sessions_from_snowflake(get_relevant_sessions_config)
    edited_sessions = _filter_unedited_sessions_using_mysql(
        sessions, FilterEditedSessionsConfig()
    )
    _run_retranscribe(edited_sessions, RetranscribeConfig())
    if state_config.save_state:
        state = State(
            already_completed_sessions=get_relevant_sessions_config.already_completed_sessions
            + edited_sessions,
            last_sealed_date=now,
        )
        state_handler.save(state)


if __name__ == "__main__":
    main()
