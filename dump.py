import boto3
import json
import pandas as pd

# Import custom files/modules
import global_vars

def table_exists(table_name):
    for table, data in global_vars.dict_tables.items():
        if(data["table_name"] == table_name):
            return True
    return False

def dump_table(dynamodb_res, table_name):
    if(not table_exists(table_name)):
        print("Table does not exist")
        return
    
    table = dynamodb_res.Table(table_name)
    response = table.scan()
    header_cols = False
    for item in response['Items']:
        for table, data in global_vars.dict_tables.items():
            if(data["table_name"] == table_name):
                for i in range(0, len(data["columns"])):
                    print(item[data["columns"][i]] + "\t", end='')
                print("")

# def dump_table(dynamodb_client, table_name):
#     if(not table_exists(table_name)):
#         print("Table does not exist")
#         return
    
#     table_json = global_vars.json_dir + table_name.replace('spreisig_', '') + ".json"
#     df = pd.read_json(table_json)
#     print(df.T)