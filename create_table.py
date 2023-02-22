# Authour: Samantha Preisig
# File: create_table.py (module)
# Brief: creates and populates DynamoDB tables

import boto3
import csv
import json
from mergedeep import merge

# Import custome files/modules
import global_vars
from delete_table import delete_table
from load_records import bulk_load

# Purpose: get ISO3 code given the country_name
# Params:
#   - country_name: name of the country
# Returns: ISO3 for country_name, None if country_name DNE in shortlist_capitals.json
# or if shortlist_capitals.json DNE in data/ directory
def get_iso3(country_name):
    # Opening shortlist_capitals.json as it's one of the smaller json files (small
    # dicts for each country). The key for each country dict is the country's ISO3 value
    # which is convenient
    with open(global_vars.json_dir+"shortlist_capitals.json", "r") as json_file:
        json_dict = json.load(json_file)

        for key, value in json_dict.items():
            for item in value:
                if(item == "Country Name" and value[item] == country_name):
                    return key
    return None

# Purpose: get country name given the ISO3 code
# Params:
#   - iso3: three-letter code (example, Canada's ISO3 is 'CAN')
# Returns: country name for given ISO3, None if ISO3 has no associated country name
# or if shortlist_capitals.json DNE in data/ directory
def get_country_name(iso3):
    # Opening shortlist_capitals.json as it's one of the smaller json files (small
    # dicts for each country)
    with open(global_vars.json_dir+"shortlist_capitals.json", "r") as json_file:
        json_dict = json.load(json_file)

        item_found = False
        for key, value in json_dict.items():
            for item in value:
                if(item == "ISO3" and value[item] == iso3):
                    item_found = True
                if(item_found and item == "Country Name"):
                    return value[item]
    return None

# Purpose: returns the type ('S' => string, 'N' => number) of a given key
# Params:
#   - key: the partition or sort key of a dynamoDB table
# Returns: 'N' if key represents numerical data, 'S' otherwise ('S' is default)
def get_key_type(key):
    if(key.isnumeric() or key == "Area"):
        return 'N'
    else:
        return 'S'

# Purpose: returns a tuple (partition_key, sort_key) for a given table
# Params:
#   - table_name: the table name requiring parition and sort key information
# Returns: (partition_key, sort_key) tuple. Since sort keys are optional, sort_key
# may be returned as an empty string if no sort key exists in table_name
def get_table_keys(table_name):
    for table, data in global_vars.dict_tables.items():
        if(data["table_name"] == table_name):
            if(len(data["key_columns"]) < 2):
                return data["key_columns"][0], "" # Sort key DNE
            else:
                return data["key_columns"][0], data["key_columns"][1]

# Purpose: returns a list of languages for a given csv_file
# Params:
#   - csv_file_path: the full csv file path containing language information
# (shortlist_languages.csv)
# Returns: list of languages
def get_languages(csv_file_path):
    with open(csv_file_path, newline='', encoding="utf-8-sig") as f:
        csv_reader = csv.reader(f)
        data = list(csv_reader)

        languages = []
        first_row = 1
        for row in data:
            if(first_row):
                first_row = 0
            else:
                languages.append(list(filter(None, row[2:])))
        return languages

# Purpose: returns a list of full file paths to relevant json files for a given table.
# The first filename in the list is the destination json file when calling merge_info()
# in create_table(). Please refer to root README.md for details on the economic and non-
# economic data and their respective files (data organization)
# Params:
#   - table_name: the table name to generate json file dependency list
# Returns: list of full (relevant) json paths for table_name, or empty list if table_name
# is not "spreisig_economic" or "spreisig_non_economic"
def get_relevant_filenames(table_name):
    if(table_name == "spreisig_economic"):
        return [global_vars.json_dir+"shortlist_economic.json", global_vars.json_dir+"shortlist_curpop.json", global_vars.json_dir+"shortlist_gdppc.json"]
    if(table_name == "spreisig_non_economic"):
        return [global_vars.json_dir+"shortlist_non_economic.json", global_vars.json_dir+"shortlist_area.json", global_vars.json_dir+"shortlist_capitals.json", global_vars.json_dir+"shortlist_curpop.json", global_vars.json_dir+"shortlist_languages.json", global_vars.json_dir+"un_shortlist.json"]
    return []

# Purpose: populate global dict_tables with column header names (i.e., Country Name, ISO3, Area,
# 1970 ... 2019, etc)
# Params:
#   - json_file: json file to extract column header names
#   - table_name: name of table to populate column header names to
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

# Purpose: converts given csv file to json file
# Params:
#   - csv_file_path: the full path to the csv file to be converted (data/xxx.csv)
#   - json_file_path: the full path for the converted json file (data/json/xxx.json)
#   - table_name: the name of the dynamoDB table which will be populated with information from
# the converted json file
def csv_to_json(csv_file_path, json_file_path, table_name):
    print("Converting " + csv_file_path + " to json ...")
    data_dict = {}

    # Opening a csv file handler
    with open(csv_file_path, "r", encoding="utf-8-sig") as csv_file_handler:
        csv_reader = csv.DictReader(csv_file_handler)

        # Convert each row into a dictionary (stored in data_dict)
        # shortlist_languages.csv is a special case, with
        # some countries having multiple languages..
        if("languages" in table_name):
            languages = get_languages(csv_file_path)
            i = 0
            for row in csv_reader:
                key = row[str(get_table_keys(table_name)[0])]
                # Languages are grouped into one value - not the best way to handle this
                # TODO: the key "Languages" has a LIST of languages as its value
                row["Languages"] = ' '.join(languages[i])
                data_dict[key] = row
                i += 1
        else:
            for row in csv_reader:
                key = row[str(get_table_keys(table_name)[0])]
                data_dict[key] = row
        
    # Open a json file handler and use json.dumps method to dump the data
    with open(json_file_path, "w") as json_file_handler:
        json_file_handler.write(json.dumps(data_dict, indent=4))
    
    populate_global_columns(json_file_path, table_name)

# Purpose: converts given csv_filename to json file and places json file into
# data/json/ directory
# Params:
#   - table_name: name of table - needed for csv_to_json() function call
#   - csv_filename: csv file to be converted
def build_json(table_name, csv_filename):
    json_filename = csv_filename.replace(global_vars.data_dir, global_vars.json_dir) # json files placed in data/json/
    json_filename = json_filename.replace('.csv', '.json') # Creating json filename (replacing csv to json extension)
    csv_to_json(csv_filename, json_filename, table_name) # Convert csv to json file

# Purpose: takes a list of relevant json filenames and gathers + stores information into the destination
# json file (filename[0]). A dynamoDB table will be populated using this destination json file.
# Please refer to root README.md for details on the economic and non-economic data and their respective
# files (data organization)
# Params:
#   - filenames: relevant filenames to extract information from, with filenames[0] being the destination
# filename containing the merged economic or non-economic data
def merge_info(filenames):
    dict_list = [] # Master list, containing dicts of individual pieces of data

    # Merging economic data into shortlist_economic.json
    if(filenames[0] == global_vars.json_dir+"shortlist_economic.json"):
        for f in filenames[1:]:
            with open(f, "r") as json_file:
                json_dict = json.load(json_file)
                data_dict = {}

                for key, value in json_dict.items():
                    country_dict = {} # Gathering data for an individual country
                    for item in value:
                        if(f == global_vars.json_dir+"shortlist_curpop.json" and item == "Country Name"):
                            country_dict[item] = value[item]
                        elif(f == global_vars.json_dir+"shortlist_curpop.json" and item == "Currency"):
                            country_dict[item] = value[item]
                        elif(f == global_vars.json_dir+"shortlist_gdppc.json" and item.isnumeric() and value[item] != ''):
                            country_dict[item] = int(value[item])
                    data_dict[key] = country_dict
                    dict_list.append(data_dict) # Add individual country data to master list
        
        for d in dict_list:
            merge(data_dict, d)
    else:

        # Merging non-economic data into shortlist_non_economic.json
        for f in filenames[1:]:
            with open(f, "r") as json_file:
                json_dict = json.load(json_file)
                data_dict = {}

                for key, value in json_dict.items():
                    country_dict = {} # Gathering data for an individual country
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
                            country_dict[item] = int(float(value[item]))
                        elif(f == global_vars.json_dir+"shortlist_languages.json" and item == "Languages"):
                            country_dict[item] = value[item]
                        elif(f == global_vars.json_dir+"un_shortlist.json" and item == "Official Name"):
                            country_dict[item] = value[item]
                    # To create some consistency, if key is ISO3 instead of Country Name, the ISO3 key is converted
                    # to its equivalent Country Name and used when storing country_dict to data_dict
                    if(len(key) == 3):
                        new_key = get_country_name(key)
                        data_dict[new_key] = country_dict
                    else:
                        data_dict[key] = country_dict
                    dict_list.append(data_dict) # Add individual country data to master list

        for d in dict_list:
            merge(data_dict, d)
            
    with open(filenames[0], "w") as out_json:
        out_json.write(json.dumps(data_dict, indent=4))
    if("non_economic" in filenames[0]):
        populate_global_columns(filenames[0], "spreisig_non_economic")
    else:
        populate_global_columns(filenames[0], "spreisig_economic")

# Purpose: builds and populates a dynamoDB table
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - dynamodb_client: low-level interface for AWS services requests
#   - table_name: name of table to be created
def create_table(dynamodb_res, dynamodb_client, table_name):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    
    if table_name not in existing_tables:
        print("\nCreating "+ table_name + " ...")
        files = get_relevant_filenames(table_name)
        merge_info(files)

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
        # print("Table status:", table.table_status, table.table_name)
        table.wait_until_exists() # Wait until the table exists
        bulk_load(dynamodb_res, table_name, files[0])
        print("Table status: " + table_name + " created")
    else:
        # Create tables is only called again when new data has been added to csv files. Therefore
        # the existing tables are deleted and rebuilt using the newly converted csv to json files
        print("\n" + table_name + " already exists")
        delete_table(dynamodb_client, table_name)
        create_table(dynamodb_res, dynamodb_client, table_name)
