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
        if("languages" in table_name):
            languages = get_languages(csv_file_path)
            i = 0
            for row in csv_reader:
                key = row[str(get_table_keys(table_name)[0])]
                row["Languages"] = ' '.join(languages[i])
                data_dict[key] = row
                i += 1
        else:
            for row in csv_reader:
                key = row[str(get_table_keys(table_name)[0])]
                data_dict[key] = row
        
    # Open a json file handler and use json.dumps method to dump the data
    with open(json_file_path, 'w', encoding='utf-8') as json_file_handler:
        json_file_handler.write(json.dumps(data_dict, indent=4))
    
    populate_global_columns(json_file_path, table_name)

def get_languages(csv_file_path):
    with open(csv_file_path, newline='', encoding='utf8') as f:
        csv_reader = csv.reader(f)
        data = list(csv_reader)

        languages = []
        first_row = 1
        for row in data:
            if(first_row):
                first_row = 0
            else:
                languages.append(list(filter(None, row[2:])))
        # print(languages)
        return languages

def populate_global_columns(json_file, table_name):
    columns_added = False
    with open(json_file, "r") as f:
        data = json.load(f)
        for data_key, data_value in data.items():
            for key in data_value:
                for table, table_data in global_vars.dict_tables.items():
                    if(table_data["table_name"] == table_name and not columns_added):
                        table_data["columns"].append(key)
            columns_added = True

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
    json_filename = csv_filename.replace(global_vars.data_dir, global_vars.json_dir) # json files placed in data/json/
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

    if(json_filename == global_vars.json_dir+"shortlist_gdppc.json"):
        build_economic_table(dynamodb_res, dynamodb_client, "spreisig_shortlist_economic")

# Economic data:
#   - GDPPC (shortlist_gdppc.json)
#   - Currency (shortlist_curpop.json) -> needs to be split into econ and non-econ json files
def build_economic_table(dynamodb_res, dynamodb_client, table_name):
    
    # files[0] contains the json file with merged content, files[1:] are json files to be merged
    files = [global_vars.json_dir+"shortlist_economic.json", global_vars.json_dir+"shortlist_curpop.json", global_vars.json_dir+"shortlist_gdppc.json"]
    merge_json(files)

    # # Creating economic table
    # existing_tables = dynamodb_client.list_tables()['TableNames']
    # if table_name not in existing_tables:
    #     part_key, sort_key = get_table_keys(table_name)
    #     if(sort_key == ""):
    #         table = dynamodb_res.create_table(
    #             TableName=table_name,
    #             KeySchema=[
    #                 {
    #                     'AttributeName': part_key,
    #                     'KeyType': 'HASH' # Partition key
    #                 }
    #             ],
    #             AttributeDefinitions=[
    #                 {
    #                     'AttributeName': part_key,
    #                     'AttributeType': get_key_type(part_key)
    #                 }
    #             ],
    #             ProvisionedThroughput={
    #                 'ReadCapacityUnits': 10,
    #                 'WriteCapacityUnits': 10
    #             }
    #         )
    #     else:
    #         table = dynamodb_res.create_table(
    #             TableName=table_name,
    #             KeySchema=[
    #                 {
    #                     'AttributeName': part_key,
    #                     'KeyType': 'HASH' # Partition key
    #                 },
    #                 {
    #                     'AttributeName': sort_key,
    #                     'KeyType': 'RANGE' # Sort key
    #                 }
    #             ],
    #             AttributeDefinitions=[
    #                 {
    #                     'AttributeName': part_key,
    #                     'AttributeType': get_key_type(part_key)
    #                 },
    #                 {
    #                     'AttributeName': sort_key,
    #                     'AttributeType': get_key_type(sort_key)
    #                 },
    #             ],
    #             ProvisionedThroughput={
    #                 'ReadCapacityUnits': 10,
    #                 'WriteCapacityUnits': 10
    #             }
    #         )
    #     # Print table information
    #     print("Table status:", table.table_status, table.table_name)
    #     table.wait_until_exists() # Wait until the table exists

# The first filename in filenames list is the filename that contains the merge
def merge_json(filenames):

    data_dict1 = {}
    data_dict2 = {}
    with open(filenames[1], 'r') as in_json_file1:
        json_dict1 = {}
        json_dict1 = json.load(in_json_file1)

        # data_dict1 = {}
        for obj, val in json_dict1.items():
            data_dict1[obj] = val

    with open(filenames[0], 'w') as out_file:
        out_file.write(json.dumps(data_dict1, indent=4))

    with open(filenames[2], 'r') as in_json_file2:
        json_dict2 = {}
        json_dict2 = json.load(in_json_file2)

        # data_dict2 = {}
        for obj, val in json_dict2.items():
            data_dict2[obj] = val
    # merged_data_dict = data_dict1.copy()
    # merged_data_dict.update(data_dict2)
    # merged_data_dict = {**data_dict1, **data_dict2}
    # print(merged_data_dict)

    with open(filenames[0], 'a') as out_file:
        out_file.write(json.dumps(data_dict2, indent=4))

# Non-economic data:
#   - Area (shortlist_area.json)
#   - Capital (shortlist_capitals.json)
#   - Population (shortlist_curpop.json)
#   - Languages (shortlist_languages.json)
#   - ISO2/Official Name (un_shortlist.json)
# def build_noneconomic_table():

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