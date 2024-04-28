#!/bin/bash
mkdir -p package
rm -r ./package/*
rm retranscribe-unedited.zip
cd package
zip -r ../retranscribe-unedited.zip .
cd ..
zip retranscribe-unedited.zip retranscribe_unedited.py
aws s3 cp retranscribe-unedited.zip s3://retranscribe-unedited-sessions/lambda-zip/