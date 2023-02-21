#!/usr/bin/env python3

import configparser
import os
import sys
import boto3
import readline
from botocore.exceptions import ClientError

# Import custom files/modules
import global_vars
from create_table import build_json, create_table
from cli_commands import *

# Purpose: initializing global variables declared in global_vars.py. These
# variables are made global to all modules in the current directory
def set_globals():
    global_vars.init() # Declaring global variables

    # Initialization
    global_vars.dict_tables = {
        "shortlist_area.csv": {"table_name": "spreisig_shortlist_area", "key_columns": ["ISO3"], "columns": []},
        "shortlist_capitals.csv": {"table_name": "spreisig_shortlist_capitals", "key_columns": ["ISO3"], "columns": []},
        "shortlist_curpop.csv": {"table_name": "spreisig_shortlist_curpop", "key_columns": ["Country Name"], "columns": []},
        "shortlist_gdppc.csv": {"table_name": "spreisig_shortlist_gdppc", "key_columns": ["Country Name"], "columns": []},
        "shortlist_languages.csv": {"table_name": "spreisig_shortlist_languages", "key_columns": ["ISO3"], "columns": []},
        "un_shortlist.csv": {"table_name": "spreisig_un_shortlist", "key_columns": ["ISO3"], "columns": []},
        "shortlist_economic": {"table_name": "spreisig_economic", "key_columns": ["Country Name"], "columns": []},
        "shortlist_non_economic": {"table_name": "spreisig_non_economic", "key_columns": ["ISO3"], "columns": []}
    }
    global_vars.data_dir = "data/"
    global_vars.json_dir = global_vars.data_dir+"json/"
    global_vars.help_dir = "help/"
    global_vars.add_records_file = global_vars.data_dir+"add_records.txt"
    global_vars.delete_records_file = global_vars.data_dir+"delete_records.txt"
    global_vars.missing_info_file = global_vars.data_dir+"missing_info.txt"

# Purpose: establishes an AWS session by parsing config file (S5-S3.conf) and setting
# up client and resources
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

# Purpose: iterates through data/ directory and converts each csv files to
# json, placing each converted json file into data/json/ directory. After
# all csv files have been converted to json files, create_tables() is called
# to create spreisig_economic and spreisig_non_economic tables containing organized
# information using the converted json files
def build_tables(dynamodb_res, dynamodb_client):
    filenames = os.listdir(global_vars.data_dir)

    # Removing filenames within the data directory that are not csv files
    filenames.remove(global_vars.add_records_file.replace(global_vars.data_dir, ''))
    filenames.remove(global_vars.delete_records_file.replace(global_vars.data_dir, ''))
    filenames.remove(global_vars.missing_info_file.replace(global_vars.data_dir, ''))
    filenames.remove("json")
    filenames.remove("README.md")

    # Looping through each csv file within the data directory and calling build_json to
    # convert the csv file into a json file
    for filename in filenames:
        table_name = ("spreisig_"+filename).replace('.csv', '')
        build_json(table_name, global_vars.data_dir+filename)
    # Creating economic and non-economic tables
    create_table(dynamodb_res, dynamodb_client, "spreisig_economic")
    create_table(dynamodb_res, dynamodb_client, "spreisig_non_economic")

# Purpose: DRIVER
# Initializes global variables, establishes AWS session, builds economic and non-economic
# tables, and launches CLI for table manipulation and report production
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

        elif(command == "delete_table"):
            cmd_delete_table(dynamodb_client, args)

        elif(command == "add_data"):
            cmd_add_data()
            build_tables(dynamodb_res, dynamodb_client) # Rebuilding tables with updated csv files

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