import os
import sys

# Import custom files/modules
from create_table import create_table
from delete_table import delete_table
from load_records import add_data, add_record
from delete_record import delete_record
from dump import dump_table
from build_reports import build_country_report, build_global_report

# Purpose: reads help text files (found in help/ directory) given by args. If args is emtpy,
# help/default.txt is read and printed to stdout
# Params:
#   - args: (len=1) contains the name of command the user needs help with
def cmd_help(args):
    if args:
        try:
            with open(f'help/{args[0]}.txt') as f:
                print(f.read())
        except FileNotFoundError as e:
            print("File not found")
    else:
        with open('help/default.txt') as f:
            print(f.read())

# Purpose: builds economic and non-economic dynamoDB tables in the
# event the user deletes a table in a single CLI-run
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - dynamodb_client: low-level interface for AWS services requests
#   - args: (len=1) contains the name of the table to be created
def cmd_create_table(dynamodb_res, dynamodb_client, args):
    if(len(args) != 1):
        print("Invalid arguments. Enter `help create_table` for valid arguments")
        return
    create_table(dynamodb_res, dynamodb_client, args[0])

# Purpose: calls delete_table to delete the given table (args)
# Params:
#   - dynamodb_client: low-level interface for AWS services requests
#   - args: (len=1) contains the name of the table to be deleted
def cmd_delete_table(dynamodb_client, args):
    if(len(args) != 1):
        print("Invalid arguments. Enter `help delete_table` for valid arguments")
        return
    delete_table(dynamodb_client, args[0])

# Purpose: calls add_data to add missing information to csv file(s)
def cmd_add_data():
    add_data()

# TODO
def cmd_add_record(dynamodb_res):
    add_record(dynamodb_res)

# TODO
def cmd_delete_record(dynamodb_res):
    delete_record(dynamodb_res)

# TODO
def cmd_dump(dynamodb_res, args): #TODO
    if(len(args) != 1):
        print("Invalid arguments. Enter `help dump` for valid arguments")
        return
    dump_table(dynamodb_res, args[0])

# Purpose: builds pdf report based on args (refer to help/build_report.txt for
# possible args)
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - args: list of arguments + flags
def cmd_build_report(dynamodb_res, args):
    if(len(args) != 2):
        print("Invalid arguments. Enter `help build_report` for valid arguments")
        return
    if(args[0] == "-c"):
        build_country_report(dynamodb_res, args[1]) # Country name is passed
    elif(args[0] == "-g"):
        build_global_report(dynamodb_res, args[1]) # Year is passed
    else:
        print("Invalid arguments. Enter `help build_report` for valid arguments")
        return