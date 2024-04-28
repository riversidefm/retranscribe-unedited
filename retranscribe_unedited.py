from datetime import datetime
import time
from typing import List
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
    start_date: datetime = Field(default_factory=datetime.now)


class FilterEditedSessionsConfig(BaseSettings):
    rds_host: str
    rds_port: int
    rds_database: str
    rds_user: str
    rds_password: str
    rds_sentences_table_fqdn: str = "transcriptions.sentences"
    
class RetranscribeConfig(BaseSettings):
    api_url: str = 'https://riverside.fm/api/2/recordings/transcript'
    wait_between_requests_secs: float = 0.5
    
    


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
    query = f"""SELECT distinct(session_id) 
    FROM {table_name} 
    WHERE session_id in ({','.join(map(lambda s: f"'{s}'", session_ids))}) and (edited_words_v2 is not NULL) group by session_id"""
    result = connection.execute(text(query))
    sessions = [row[0] for row in result]
    return sessions
    

def get_relevant_sessions(config: GetRelevantSessionsConfig) -> List[str]:
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
    query = _get_sf_query(config.start_date)
    sf_rows = session.sql(query).collect()
    session_ids = [row["SESSION_ID"] for row in sf_rows]
    return session_ids


def get_unedited_sessions(
    sessions: List[str], config: FilterEditedSessionsConfig
) -> List[str]:
    rds_conn = _connect_to_mysql(
        config.rds_host,
        config.rds_port,
        config.rds_database,
        config.rds_user,
        config.rds_password,
    )
    edited_sessions = _fetch_data_by_session_id(
        rds_conn, config.rds_sentences_table_fqdn, sessions
    )
    rds_conn.close()
    return list(filter(lambda session: session not in edited_sessions, sessions))


def _build_retranscribe_url(api_url: str, session_id: str) -> str:
    return f"{api_url}/{session_id}"

def run_retranscribe(session_ids: List[str], config: RetranscribeConfig) -> None:
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


def main() -> None:
    sessions = get_relevant_sessions(GetRelevantSessionsConfig())
    edited_sessions = get_unedited_sessions(sessions, FilterEditedSessionsConfig())
    run_retranscribe(edited_sessions, RetranscribeConfig())


def _get_sf_query(date: datetime):
    date_str = date.strftime("%Y-%m-%d")
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

if __name__ == "__main__":
    main()
