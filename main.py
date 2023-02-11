#!/usr/bin/env python3

import configparser
import os
import sys
import boto3
import readline
from botocore.exceptions import ClientError

# Import custom files/modules
import global_vars
from create_table import create_table
# from delete_table import delete_table
# from load_records import add_record
from cli_functions import *

def set_globals():
    global_vars.init()
    global_vars.dict_tables = {
        "shortlist_area.csv": {"table_name": "spreisig_shortlist_area", "key_columns": ["ISO3", "Area"]},
        "shortlist_capitals.csv": {"table_name": "spreisig_shortlist_capitals", "key_columns": ["ISO3", "Capital"]},
        "shortlist_curpop.csv": {"table_name": "spreisig_shortlist_curpop", "key_columns": ["\ufeffCountry Name"]},
        "shortlist_gdppc.csv": {"table_name": "spreisig_shortlist_gdppc", "key_columns": ["\ufeffCountry Name"]},
        # "shortlist_languages.csv": {"table_name": "spreisig_shortlist_languages", "key_columns": ["ISO3"]},
        "un_shortlist.csv": {"table_name": "spreisig_un_shortlist", "key_columns": ["ISO3", "Official Name"]}
    }
    global_vars.data_dir = "data/"
    global_vars.help_dir = "help/"
    global_vars.add_record_file = "add_record.txt"

def config_and_setup():
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
        return dynamodb_res, dynamodb_client

    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print("User already exists")
        else:
            print("Unexpected error: %s" % e)

def build_tables(dynamodb_res, dynamodb_client):
    filenames = os.listdir(global_vars.data_dir)
    for filename in filenames:
        table_name = ("spreisig_"+filename).replace('.csv', '')
        create_table(dynamodb_res, dynamodb_client, table_name, global_vars.data_dir+filename)

def main():
    # Setting global variables, AWS configuration, and initial bulk creation of tables
    set_globals()
    dynamodb_res, dynamodb_client = config_and_setup()
    build_tables(dynamodb_res, dynamodb_client)

    # CLI
    print("\nWelcome to Sam's custom AWS CLI\nFor help command, type `help`\nTo stop CLI, type `quit`")
    while True:
        cli = input("> ").split()
        command = cli[0]
        args = cli[1:]

        if(command == "help"):
            help(args)

        elif(command == "create_new_table"):
            cmd_create_table(dynamodb_res, dynamodb_client, args)

        elif(command == "delete_table"):
            cmd_delete_table(dynamodb_client, args)

        elif(command == "add_record"):
            cmd_add_record(dynamodb_res, args)

        elif(command == "quit"):
            break

        else:
            print("Not a valid command. Enter `help` for a list of valid commands")
    # delete_table(dynamodb_client, "spreisig_shortlist_area")
    # delete_table(dynamodb_client, "spreisig_shortlist_capitals")
    # delete_table(dynamodb_client, "spreisig_shortlist_curpop")
    # delete_table(dynamodb_client, "spreisig_shortlist_gdppc")
    # delete_table(dynamodb_client, "spreisig_un_shortlist")

if __name__ == "__main__":
    main()