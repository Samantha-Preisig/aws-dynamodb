#!/usr/bin/env python3

import configparser
import os
import sys
import boto3
import readline
from botocore.exceptions import ClientError

# Import custom files/modules
import global_vars
from create_table import build_json, create_tables
from cli_commands import *

def set_globals():
    global_vars.init()
    global_vars.dict_tables = {
        "shortlist_area.csv": {"table_name": "spreisig_shortlist_area", "key_columns": ["ISO3"], "columns": []},
        "shortlist_capitals.csv": {"table_name": "spreisig_shortlist_capitals", "key_columns": ["ISO3"], "columns": []},
        "shortlist_curpop.csv": {"table_name": "spreisig_shortlist_curpop", "key_columns": ["\ufeffCountry Name"], "columns": []},
        "shortlist_gdppc.csv": {"table_name": "spreisig_shortlist_gdppc", "key_columns": ["\ufeffCountry Name"], "columns": []},
        "shortlist_languages.csv": {"table_name": "spreisig_shortlist_languages", "key_columns": ["ISO3"], "columns": []},
        "un_shortlist.csv": {"table_name": "spreisig_un_shortlist", "key_columns": ["ISO3"], "columns": []},
        "shortlist_economic": {"table_name": "spreisig_economic", "key_columns": ["\ufeffCountry Name"], "columns": []},
        "shortlist_non_economic": {"table_name": "spreisig_non_economic", "key_columns": ["ISO3"], "columns": []}
    }
    global_vars.data_dir = "data/"
    global_vars.json_dir = global_vars.data_dir+"json/"
    global_vars.help_dir = "help/"
    global_vars.add_records_file = global_vars.data_dir+"add_records.txt"
    global_vars.delete_records_file = global_vars.data_dir+"delete_records.txt"

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

    filenames.remove(global_vars.add_records_file.replace(global_vars.data_dir, ''))
    filenames.remove(global_vars.delete_records_file.replace(global_vars.data_dir, ''))
    filenames.remove("json")
    filenames.remove("README.md")

    for filename in filenames:
        table_name = ("spreisig_"+filename).replace('.csv', '')
        build_json(table_name, global_vars.data_dir+filename)
    create_tables(dynamodb_res, dynamodb_client)

def main():
    # Setting global variables, AWS configuration, and initial bulk creation of tables
    set_globals()
    dynamodb_res, dynamodb_client = config_and_setup()
    build_tables(dynamodb_res, dynamodb_client)

    # CLI
    print("\nWelcome to Sam's custom AWS CLI\nFor help command, type `help`\nTo stop CLI, type `quit`\n")
    while True:
        cli = input("> ").split()
        command = cli[0]
        args = cli[1:]

        if(command == "help"):
            cmd_help(args)

        elif(command == "create_new_table"):
            cmd_create_table(dynamodb_res, dynamodb_client, args)

        elif(command == "delete_table"):
            cmd_delete_table(dynamodb_client, args)

        elif(command == "add_record"):
            cmd_add_record(dynamodb_res)

        elif(command == "delete_record"):
            cmd_delete_record(dynamodb_res)

        elif(command == "dump"):
            cmd_dump(dynamodb_res, args)

        elif(command == "build_report"):
            cmd_build_report(dynamodb_res, args)

        elif(command == "quit"):
            break

        else:
            print("Not a valid command. Enter `help` for a list of valid commands")

if __name__ == "__main__":
    main()