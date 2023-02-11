import os
import sys
import boto3

# Import modules
from delete_table import delete_table

def help(args):
    if args:
        try:
            with open(f'help/{args[0]}.txt') as f:
                print(f.read())
        except FileNotFoundError as e:
            print("Not a valid command. Enter `help` for a list of valid commands")
    else:
        with open('help/default.txt') as f:
            print(f.read())

def cmd_delete_table(dynamodb_client, args):
    if(len(args) != 1):
        print("Not a valid command. Enter `help delete_table` for a list of valid commands")
        return
    else:
        delete_table(dynamodb_client, args[0])

# def cmd_add_record(dynamodb_client, args):
#     if(len(args) != 1):
#         print("Not a valid command. Enter `help add_record` for a list of valid commands")
#         return
#     else:
#         add_record(args[0])