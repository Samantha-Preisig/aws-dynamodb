import boto3
import json

# Import custom files/modules
import global_vars

# Initial load of data from csv-json files
def bulk_load(dynamodb_res, table_name, json_filename):
    table = dynamodb_res.Table(table_name)
    
    with open(json_filename, "r") as json_file:
        data = json.load(json_file)
        for key, value in data.items():
            data[key] = {key: value for key, value in data[key].items() if key} # **Only reads FIRST language listed if country has more than 1 language**
            table.put_item(
                Item=data[key]
            )

def load(dynamodb_res, table_name, record_dict):
    table = dynamodb_res.Table(table_name)
    table.put_item(
        Item=record_dict
    )

def add_record(dynamodb_res):
    with open(global_vars.add_records_file, "r") as f:
        lines = f.readlines()

    table_name = ""
    for line in lines:
        filename = line.split(':')[0]
        table_name = "spreisig_"+(filename.replace('.csv', ''))
        record_data = line.split(':')[1]
        record_data = record_data.split(',')
        record_dict = {}
        for data in record_data:
            if(len(data.split()) == 3): # Example, key = Country Name, value = Canada
                key = data.split()[0] + " " + data.split()[1]
                if(key == "Country Name" and not record_dict):
                    key = "\ufeff"+key
                value = data.split()[2]
            else:
                key, value = data.split()
            record_dict[key] = value
        load(dynamodb_res, table_name, record_dict)
        print("Record '" + line.replace('\n', '') + "' added")

def load_economic(dynamodb_res, table_name, json_filename):
    table = dynamodb_res.Table(table_name)
    
    with open(json_filename, "r") as json_file:
        data = json.load(json_file)

        for key, value in data.items():
            data[key] = {key: value for key, value in data[key].items() if key}
            table.put_item(
                Item=data[key]
            )

# def add_to_existing_record(filename):
