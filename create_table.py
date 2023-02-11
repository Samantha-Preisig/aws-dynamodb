import boto3
import csv
import json
import pandas as pd # Not used (yet..?)

# Import custome files/modules
import global_vars
from load_records import bulk_load

def csv_to_json(csv_file_path, json_file_path, table_name): # TODO: Find reference
    print("\nConverting csv to json for " + table_name + " ...")
    data_dict = {} # Create a dictionary

    # Opening a csv file handler
    with open(csv_file_path, encoding='utf-8') as csv_file_handler:
        csv_reader = csv.DictReader(csv_file_handler)

        # Convert each row into a dictionary and add the converted data to data_variable
        for row in csv_reader:
            key = row[str(get_table_keys(table_name)[0])]
            data_dict[key] = row
        
    # Open a json file handler and use json.dumps method to dump the data
    with open(json_file_path, 'w', encoding='utf-8') as json_file_handler:
        json_file_handler.write(json.dumps(data_dict, indent=4))

def get_table_keys(table_name):
    for table, data in global_vars.dict_tables.items():
        if(data["table_name"] == table_name):
            if(len(data["key_columns"]) < 2):
                return data["key_columns"][0], ""
            else:
                return data["key_columns"][0], data["key_columns"][1]

def get_key_type(key): # TODO: need area to be N instead of S
    # if(key == "Area"):
    #     return 'N'
    # else:
    #     return 'S'
    return 'S'

def create_table(dynamodb_res, dynamodb_client, table_name, csv_filename):
    json_filename = csv_filename.replace(global_vars.data_dir, '') # json files are created at root (not within data/)
    json_filename = json_filename.replace('.csv', '.json') # Creating json filename (replacing csv to json extension)
    csv_to_json(csv_filename, json_filename, table_name) # Convert csv to json file

    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        part_key, sort_key = get_table_keys(table_name)
        if(sort_key == ""):
            table = dynamodb_res.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': part_key,
                        'KeyType': 'HASH' # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': part_key,
                        'AttributeType': get_key_type(part_key)
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
        else:
            table = dynamodb_res.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': part_key,
                        'KeyType': 'HASH' # Partition key
                    },
                    {
                        'AttributeName': sort_key,
                        'KeyType': 'RANGE' # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': part_key,
                        'AttributeType': get_key_type(part_key)
                    },
                    {
                        'AttributeName': sort_key,
                        'AttributeType': get_key_type(sort_key)
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
        # Print table information
        print("Table status:", table.table_status, table.table_name)
        table.wait_until_exists() # Wait until the table exists
        
        # Load data from json file into table
        bulk_load(dynamodb_res, table_name, json_filename)
        # print("Table item count: ", table.item_count) # Print item count for table
    else:
        print(table_name + " already exists")

def create_new_table(dynamodb_res, dynamodb_client, table_name, part_key, sort_key):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        if(sort_key == ""):
            table = dynamodb_res.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': part_key,
                        'KeyType': 'HASH' # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': part_key,
                        'AttributeType': get_key_type(part_key)
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
        else:
            table = dynamodb_res.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': part_key,
                        'KeyType': 'HASH' # Partition key
                    },
                    {
                        'AttributeName': sort_key,
                        'KeyType': 'RANGE' # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': part_key,
                        'AttributeType': get_key_type(part_key)
                    },
                    {
                        'AttributeName': sort_key,
                        'AttributeType': get_key_type(sort_key)
                    },
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            )
        # Print table information
        print("Table status:", table.table_status, table.table_name)
        table.wait_until_exists() # Wait until the table exists
    else:
        print(table_name + " already exists")