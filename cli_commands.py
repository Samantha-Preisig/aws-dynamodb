import os
import sys

# Import custom files/modules
from delete_table import delete_table
from load_records import add_data, add_record
from delete_record import delete_record
from dump import dump_table
from build_reports import build_country_report, build_global_report

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

def cmd_delete_table(dynamodb_client, args):
    if(len(args) != 1):
        print("Invalid arguments. Enter `help delete_table` for valid arguments")
        return
    delete_table(dynamodb_client, args[0])

def cmd_add_data():
    add_data()

def cmd_add_record(dynamodb_res):
    add_record(dynamodb_res)

def cmd_delete_record(dynamodb_res):
    delete_record(dynamodb_res)

def cmd_dump(dynamodb_res, args): #TODO
    if(len(args) != 1):
        print("Invalid arguments. Enter `help dump` for valid arguments")
        return
    dump_table(dynamodb_res, args[0])

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