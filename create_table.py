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
    with open(csv_file_path, 'r', encoding='utf-8-sig') as csv_file_handler:
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
    with open(json_file_path, 'w') as json_file_handler:
        json_file_handler.write(json.dumps(data_dict, indent=4))
    
    populate_global_columns(json_file_path, table_name)

def get_languages(csv_file_path):
    with open(csv_file_path, newline='', encoding='utf-8-sig') as f:
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

def get_country_name(iso3):
    with open(global_vars.json_dir+"shortlist_capitals.json", 'r') as json_file:
        json_dict = json.load(json_file)

        item_found = False
        for key, value in json_dict.items():
            for item in value:
                if(item == "ISO3" and value[item] == iso3):
                    item_found = True
                if(item_found and item == "Country Name"):
                    return value[item]
    return None

# Economic data:
#   - GDPPC (shortlist_gdppc.json)
#   - Currency (shortlist_curpop.json) -> needs to be split into econ and non-econ json files

# Non-economic data:
#   - Area (shortlist_area.json)
#   - Capital (shortlist_capitals.json)
#   - Population (shortlist_curpop.json)
#   - Languages (shortlist_languages.json)
#   - ISO2/Official Name (un_shortlist.json)

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
                    if(item1 == "Country Name" or item1 == "Currency"):
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
                        elif(f == global_vars.json_dir+"shortlist_area.json" and item == "Country Name"):
                            country_dict[item] = value[item]
                        elif(f == global_vars.json_dir+"shortlist_area.json" and item == "Area"):
                            country_dict[item] = int(value[item])
                        elif(f == global_vars.json_dir+"shortlist_capitals.json" and item == "Capital"):
                            country_dict[item] = value[item]
                        elif(f == global_vars.json_dir+"shortlist_curpop.json" and item.isnumeric() and value[item] != ''):
                            country_dict[item] = int(value[item])
                        elif(f == global_vars.json_dir+"shortlist_languages.json" and item == "Languages"):
                            country_dict[item] = value[item]
                        elif(f == global_vars.json_dir+"un_shortlist.json" and item == "Official Name"):
                            country_dict[item] = value[item]
                    # if(len(key) != 3):
                    #     new_key = get_iso3(key)
                    #     data_dict[new_key] = country_dict
                    # else:
                    #     data_dict[key] = country_dict
                    if(len(key) == 3):
                        new_key = get_country_name(key)
                        data_dict[new_key] = country_dict
                    else:
                        data_dict[key] = country_dict
                    dict_list.append(data_dict)

        for d in dict_list:
            merge(data_dict, d)
            
        with open(filenames[0], 'w') as out_json:
            out_json.write(json.dumps(data_dict, indent=4))

def get_iso3(country_key):
    with open(global_vars.json_dir+"shortlist_capitals.json", 'r') as json_file:
        json_dict = json.load(json_file)

        for key, value in json_dict.items():
            for item in value:
                if(item == "Country Name" and value[item] == country_key):
                    return key
    return None