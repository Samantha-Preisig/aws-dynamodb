#!/usr/bin/env python3

import configparser
import os
import sys
import boto3
from botocore.exceptions import ClientError

# Import modules
from create_table import *

# AWS access key id and secret access key information found in configuration file (S5-S3.conf)
config = configparser.ConfigParser()
config.read("S5-S3.conf")
aws_access_key_id = config['default']['aws_access_key_id']
aws_secret_access_key = config['default']['aws_secret_access_key']

try:

    # Establish an AWS session
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    # Set up client and resources
    dynamodb_res = session.resource('dynamodb', region_name='ca-central-1')
    dynamodb_client = session.client('dynamodb', region_name='ca-central-1')

except ClientError as e:
    if e.response['Error']['Code'] == 'EntityAlreadyExists':
        print("User already exists")
    else:
        print("Unexpected error: %s" % e)

create_table(dynamodb_res, dynamodb_client, "spreisig_shortlist_area", "shortlist_area.csv")
create_table(dynamodb_res, dynamodb_client, "spreisig_shortlist_capitals", "shortlist_capitals.csv")
create_table(dynamodb_res, dynamodb_client, "spreisig_shortlist_languages", "shortlist_languages.csv")
create_table(dynamodb_res, dynamodb_client, "spreisig_un_shortlist", "un_shortlist.csv")