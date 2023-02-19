import os
import sys
# import boto3

# Import custom files/modules
from delete_table import delete_table
from load_records import add_record
from delete_record import delete_record
from dump import dump_table

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

def cmd_create_table(dynamodb_res, dynamodb_client, args):
    if(len(args) == 2):
        create_new_table(dynamodb_res, dynamodb_client, args[0], args[1], "")
    elif(len(args) == 3):
        create_new_table(dynamodb_res, dynamodb_client, args[0], args[1], args[2])
    else:
        print("Invalid arguments. Enter `help create_new_table` for valid arguments")

def cmd_delete_table(dynamodb_client, args):
    if(len(args) != 1):
        print("Invalid arguments. Enter `help delete_table` for valid arguments")
        return
    else:
        delete_table(dynamodb_client, args[0])

def cmd_add_record(dynamodb_res):
    add_record(dynamodb_res)

def cmd_delete_record(dynamodb_res):
    delete_record(dynamodb_res)

def cmd_dump(dynamodb_res, args):
    if(len(args) != 1):
        print("Invalid arguments. Enter `help dump` for valid arguments")
        return
    dump_table(dynamodb_res, args[0])