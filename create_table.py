import boto3
import csv
import json
import pandas as pd # Not used (yet..?)
from mergedeep import merge

# Import custome files/modules
import global_vars
from load_records import bulk_load

def csv_to_json(csv_file_path, json_file_path, table_name): # TODO: Find reference
    print("Converting csv to json for " + table_name + " ...")
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

def get_key_type(key):
    if(key.isnumeric() or key == "Area"):
        return 'N'
    else:
        return 'S'

def build_json(table_name, csv_filename):
    json_filename = csv_filename.replace(global_vars.data_dir, global_vars.json_dir) # json files placed in data/json/
    json_filename = json_filename.replace('.csv', '.json') # Creating json filename (replacing csv to json extension)
    csv_to_json(csv_filename, json_filename, table_name) # Convert csv to json file

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
        
    #     # Load data from json file into table
    #     bulk_load(dynamodb_res, table_name, json_filename)
    #     # print("Table item count: ", table.item_count) # Print item count for table
    # else:
    #     print(table_name + " already exists")

    # if(json_filename == global_vars.json_dir+"shortlist_gdppc.json"):
    #     build_economic_table(dynamodb_res, dynamodb_client, "spreisig_shortlist_economic")
    # if(json_filename == global_vars.json_dir+"un_shortlist.json"):
    #     build_noneconomic_table(dynamodb_res, dynamodb_client)

def get_relevant_filenames(table_name):
    if(table_name == "spreisig_economic"):
        return [global_vars.json_dir+"shortlist_economic.json", global_vars.json_dir+"shortlist_curpop.json", global_vars.json_dir+"shortlist_gdppc.json"]
    if(table_name == "spreisig_non_economic"):
        return [global_vars.json_dir+"shortlist_non_economic.json", global_vars.json_dir+"shortlist_area.json", global_vars.json_dir+"shortlist_capitals.json", global_vars.json_dir+"shortlist_curpop.json", global_vars.json_dir+"shortlist_languages.json", global_vars.json_dir+"un_shortlist.json"]
    return []

def create_tables(dynamodb_res, dynamodb_client):
    table_names = ["spreisig_economic", "spreisig_non_economic"]
    existing_tables = dynamodb_client.list_tables()['TableNames']
    
    for table_name in table_names:
        if table_name not in existing_tables:
            print("\nBuildling "+ table_name + " ...")
            # files[0] contains the json file with merged content, files[1:] are json files to be merged
            files = get_relevant_filenames(table_name)
            merge_information(files)

            part_key, sort_key = get_table_keys(table_name)
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
            # Print table information
            print("Table status:", table.table_status, table.table_name)
            table.wait_until_exists() # Wait until the table exists
            bulk_load(dynamodb_res, table_name, files[0])
            # bulk_load(dynamodb_res, table_name, global_vars.json_dir+"shortlist_non_economic.json")
        else:
            print(table_name + " already exists")

# The first filename in filenames list is the filename that contains the merge
def merge_information(filenames):
    data_dict = {}
    if(filenames[0] == global_vars.json_dir+"shortlist_economic.json"):
        with open(filenames[1], 'r') as json_curpop, open(filenames[2], 'r') as json_gdppc:
            curpop_dict = json.load(json_curpop)
            gdppc_dict = json.load(json_gdppc)

            for key1, value1 in curpop_dict.items():
                country_dict = {}
                for item1 in value1:
                    if(item1 == "\ufeffCountry Name" or item1 == "Currency"):
                        country_dict[item1] = value1[item1]
                    
                    for key2, value2 in gdppc_dict.items():
                        for item2 in value2:
                            if(item2.isnumeric()):
                                country_dict[item2] = value2[item2]
                data_dict[key1] = country_dict
            
        
        with open(filenames[0], 'w') as out_json:
            out_json.write(json.dumps(data_dict, indent=4))
    else:
        dict_list = []
        for f in filenames[1:]:
            with open(f, 'r') as json_file:
                json_dict = json.load(json_file)
                data_dict = {}

                for key, value in json_dict.items():
                    country_dict = {}
                    for item in value:
                        if(f == global_vars.json_dir+"shortlist_area.json" and item == "ISO3"):
                            country_dict[item] = value[item]
                        elif(f == global_vars.json_dir+"shortlist_area.json" and item == "Area"):
                            country_dict[item] = value[item]
                        elif(f == global_vars.json_dir+"shortlist_capitals.json" and item == "Capital"):
                            country_dict[item] = value[item]
                        elif(f == global_vars.json_dir+"shortlist_curpop.json" and item.isnumeric() and value[item] != ''):
                            country_dict[item] = value[item]
                        elif(f == global_vars.json_dir+"shortlist_languages.json" and item == "Languages"):
                            country_dict[item] = value[item]
                        elif(f == global_vars.json_dir+"un_shortlist.json" and item == "Official Name"):
                            country_dict[item] = value[item]
                    data_dict[key] = country_dict
                    # print(data_dict)
                    dict_list.append(data_dict)
        print(dict_list)

        for d in dict_list:
            merge(data_dict, d)
            
        with open(filenames[0], 'w') as out_json:
            out_json.write(json.dumps(data_dict, indent=4))

# Economic data:
#   - GDPPC (shortlist_gdppc.json)
#   - Currency (shortlist_curpop.json) -> needs to be split into econ and non-econ json files

# Non-economic data:
#   - Area (shortlist_area.json)
#   - Capital (shortlist_capitals.json)
#   - Population (shortlist_curpop.json)
#   - Languages (shortlist_languages.json)
#   - ISO2/Official Name (un_shortlist.json)
# def build_noneconomic_table(dynamodb_res, dynamodb_client):

# def merge_non_econ():
#     with open(filenames[1], 'r') as json_curpop, open(filenames[2], 'r') as json_gdppc:
#         curpop_dict = json.load(json_curpop)
#         gdppc_dict = json.load(json_gdppc)

#         data_dict = {}
#         for key1, value1 in curpop_dict.items():
#             country_dict = {}
#             for item1 in value1:
#                 if(item1 == "\ufeffCountry Name" or item1 == "Currency"):
#                     country_dict[item1] = value1[item1]
                
#                 for key2, value2 in gdppc_dict.items():
#                     for item2 in value2:
#                         if(item2.isnumeric()):
#                             country_dict[item2] = value2[item2]
#             data_dict[key1] = country_dict
    
#     with open(filenames[0], 'w') as out_json:
#         out_json.write(json.dumps(data_dict, indent=4))