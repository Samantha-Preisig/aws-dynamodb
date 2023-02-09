#!/usr/bin/env python3

import boto3
import csv
import json

dict_tables = {
    "shortlist_area.csv": {"table_name": "spreisig_shortlist_area", "key_columns": ["ISO3", "Area"]},
    "shortlist_capitals.csv": {"table_name": "spreisig_shortlist_capitals", "key_columns": ["ISO3", "Capital"]},
    "shortlist_curpop.csv": {"table_name": "spreisig_shortlist_curpop", "key_columns": ["Country Name", "Currency"]},
    "shortlist_gdppc.csv": {"table_name": "spreisig_shortlist_gdppc", "key_columns": ["Country Name"]},
    "shortlist_languages.csv": {"table_name": "spreisig_shortlist_languages", "key_columns": ["ISO3", "Language"]},
    "un_shortlist.csv": {"table_name": "spreisig_un_shortlist", "key_columns": ["ISO3", "Official Name"]}
}

def csv_to_json(csv_file_path, json_file_path, table_name): # Find reference
    print("Converting csv to json for " + table_name + " ...")
    data_dict = {} # Create a dictionary

    # Opening a csv file handler
    with open(csv_file_path, encoding='utf-8') as csv_file_handler:
        csv_reader = csv.DictReader(csv_file_handler)

        # Convert each row into a dictionary and add the converted data to data_variable
        for rows in csv_reader:
            key = rows[str(get_table_keys(table_name)[0])]
            data_dict[key] = rows
        
    # Open a json file handler and use json.dumps method to dump the data
    with open(json_file_path, 'w', encoding='utf-8') as json_file_handler:
        json_file_handler.write(json.dumps(data_dict, indent=4))

def get_table_keys(table_name):
    for table, data in dict_tables.items():
        if(data["table_name"] == table_name):
            return data["key_columns"][0], data["key_columns"][1]

def get_key_type(key):
    if(key == "Area"):
        return 'N'
    else:
        return 'S'

#### CREATING TABLE ####
def create_table(dynamodb_res, dynamodb_client, table_name, csv_filename):
    json_filename = csv_filename.replace('.csv', '.json') # Creating json filename (replacing csv to json extension)
    csv_to_json(csv_filename, json_filename, table_name) # Convert csv to json file

    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        part_key, sort_key = get_table_keys(table_name)
        # print(part_key, sort_key)
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
        # print("Table item count: ", table.item_count) # Print item count for table
    else:
        print(table_name + " already exists")
    # Load data from json file into table
    bulk_load_table(dynamodb_res, table_name, json_filename)

#### LOADING DATA FROM JSON FILE TO TABLE ####
def bulk_load_table(dynamodb_res, table_name, json_filename):
    table = dynamodb_res.Table(table_name)
    
    with open(json_filename, "r") as json_file:
        data = json.load(json_file)
            
        if(table_name == "spreisig_shortlist_area"):
            for key, value in data.items():
                print("Adding to table: ", value['ISO3'], value['Country Name'], value['Area'])
                table.put_item(
                    Item={
                        'ISO3': value['ISO3'],
                        'Country Name': value['Country Name'],
                        'Area': int(value['Area'])
                    }
                )
        elif(table_name == "spreisig_shortlist_capitals"):
            for key, value in data.items():
                print("Adding to table: ", value['ISO3'], value['Country Name'], value['Capital'])
                table.put_item(
                    Item={
                        'ISO3': value['ISO3'],
                        'Country Name': value['Country Name'],
                        'Capital': value['Capital']
                    }
                )
        elif(table_name == "spreisig_shortlist_languages"):
            for key, value in data.items():
                print("Adding to table: ", value['ISO3'], value['Country Name'], value['Languages'])
                table.put_item(
                    Item={
                        'ISO3': value['ISO3'],
                        'Country Name': value['Country Name'],
                        'Language': value['Languages']
                    }
                )
        elif(table_name == "spreisig_un_shortlist"):
            for key, value in data.items():
                print("Adding to table: ", value['ISO3'], value['Country Name'], value['Official Name'], value['ISO2'])
                table.put_item(
                    Item={
                        'ISO3': value['ISO3'],
                        'Country Name': value['Country Name'],
                        'Official Name': value['Official Name'],
                        'ISO2': value['ISO2']
                    }
                )
    
        # print("Table item count: ", table.item_count) # Print item count for table