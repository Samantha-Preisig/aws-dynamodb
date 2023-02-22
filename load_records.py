import boto3
import json
import pandas as pd

# Import custom files/modules
import global_vars

# Purpose: bulk_load is called in main.py once an AWS session is established.
# It populates the given table_name with the information provided by json_filename
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - table_name: the name of the table being populated
#   - json_filename: the json file containing the information to be loaded, either
# shortlist_economic.json or shortlist_non_economic.json
def bulk_load(dynamodb_res, table_name, json_filename):
    table = dynamodb_res.Table(table_name)

    with open(json_filename, "r") as json_file:
        data = json.load(json_file)
        for key, value in data.items():
            data[key] = {key: value for key, value in data[key].items() if key}
            table.put_item(
                Item=data[key]
            )

# Purpose: reads data/missing_info.txt line by line, parsing each line to gather
# the specific details (location and value) to add to the csv file. Please refer
# to README.md for details on missing_info.txt structure and usability
# Assumption: due to the scale of this assignment, missing_info.txt will never
# be a large file (to the point of causing increased time complexity), therefore each
# line of the file is stored for further parsing
def add_data():
    # Store all lines of missing_info.txt
    with open(global_vars.missing_info_file, "r") as f:
        lines = f.readlines()

    for line in lines:
        filename = global_vars.data_dir+line.split(":")[0]
        data = line.split(":")[1]
        df = pd.read_csv(filename)

        # First item listed is the country the missing information belongs to
        country = ""
        i = 0
        while("\'" in data.split()[i]): # If country name contains 2 or more words
            country += data.split()[i] + " "
            i += 1
        if(i == 0):
            country = data.split()[0]
            i += 1

        # key = column, value = data to be added
        key = data.split()[i]
        value = data.split()[i+1]

        mod_df = df
        # Since a country can have more than one language, the cell might already contain a language
        # and therefore won't be null. If the cell is not empty, the new language needs to be
        # added to the cell containing the list of existing languages - NOT WORKING ATM
        # if(key == "Languages"):
        #     for i, row in df.iterrows():
        #         c = str(row["Country Name"])
        #         print(row["Country Name"] == country)
        #         if(str(row["Country Name"]) == country):
        #             languages = df[key] + value
        #             df[key] = languages
        # else:
        for i, row in df.iterrows():
            if pd.isnull(row[key]):
                mod_df = df.fillna(value)
        mod_df.to_csv(filename, index=False)

# Purpose: loads a dictionary representing a new record/row to
# the given table_name
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - table_name: name of table receiving new record
#   - record_dict: dictionary representation of new record
def load(dynamodb_res, table_name, record_dict):
    table = dynamodb_res.Table(table_name)
    table.put_item(
        Item=record_dict
    )

# Purpose: reads data/add_records.txt line by line, parsing each line to gather
# the specific details (table name, partition key, and other data) to add each record
# into its specified table. Please refer to README.md for details on add_records.txt
# structure and usability
# Assumption: due to the scale of this assignment, add_records.txt will never
# be a large file (to the point of causing increased time complexity), therefore each
# line of the file is stored for further parsing
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
def add_record(dynamodb_res):
    with open(global_vars.add_records_file, "r") as f:
        lines = f.readlines()

    for line in lines:
        table_name = line.split(':')[0]
        record_data = line.split(':')[1]
        record_data = record_data.split(',')
        
        record_dict = {}
        for data in record_data:
            # If key is multiple words
            if(len(data.split()) == 3): # Example, key = Country Name, value = Canada
                key = data.split()[0] + " " + data.split()[1]
                value = data.split()[2]
            else:
                key, value = data.split()
            record_dict[key] = value
        load(dynamodb_res, table_name, record_dict)
        print("Record '" + line.replace('\n', '') + "' added")
