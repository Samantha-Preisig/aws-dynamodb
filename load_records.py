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
    # print(record_dict)
    # print(table_name)
    table = dynamodb_res.Table(table_name)
    table.put_item(
        Item=record_dict
    )

def add_record(dynamodb_res):
    # existing_tables = dynamodb_client.list_tables()['TableNames']
    # if table_name not in existing_tables:
    #     print("Cannot add record, table does not exist")
    #     return

    with open(global_vars.add_record_file, "r") as f:
        lines = f.readlines()
    record_dict = {}
    table_name = ""
    for line in lines:
        filename = line.split(':')[0]
        table_name = "spreisig_"+(filename.replace('.csv', ''))
        record_data = line.split(':')[1]
        record_data = record_data.split(',')
        # print(filename)
        # print(record_data)
        for data in record_data:
            key, value = data.split()
            record_dict[key] = value
            # print(key, value)
        # print(record_dict)
        load(dynamodb_res, table_name, record_dict)

# def add_to_existing_record(filename):
#     lines = []
#     try:
#         with open(f'{filename}', 'r') as f:
#             # print(f.read())
#             lines = f.readlines()
#     except FileNotFoundError as e:
#         print("File not found")
    
#     print(lines)
#     # for line in lines:
