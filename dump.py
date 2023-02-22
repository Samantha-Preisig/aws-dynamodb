# Authour: Samantha Preisig
# File: dump.py (module)
# Brief: displays a given DynamoDB table's data

import boto3
import json
import numpy as np
import pandas as pd

# Import custom files/modules
import global_vars

# Purpose: checks the existence of table_name against the list of existing table names
# Params:
#   - dynamodb_client: low-level interface for AWS services requests
#   - table_name: name of table being checked for existence
# Returns: True if table_name exists, False otherwise
def table_exists(dynamodb_client, table_name):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    
    if table_name not in existing_tables:
        return False
    return True

# Purpose: scans and prints a dataframe representation of the given table_name to stdout
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - dynamodb_client: low-level interface for AWS services requests
#   - table_name: name of table to be displayed
def dump_table(dynamodb_res, dynamodb_client, table_name):
    if(not table_exists(table_name)):
        print("Table does not exist")
        return
    
    # Scan entire table
    table = dynamodb_res.Table(table_name)
    response = table.scan()

    # Populating 2D array, with rows representing the table's records
    master_data = []
    column_headers = []
    for item in response["Items"]:
        for table, data in global_vars.dict_tables.items():
            if(data["table_name"] == table_name):
                column_headers = data["columns"]
                row = []
                for i in range(0, len(data["columns"])):
                    try:
                        row.append(item[data["columns"][i]])
                    except:
                        row.append("-")
                master_data.append(row)

    # Converting master_data array to numpy array in order to build
    # dataframe and print table information to stdout
    master_np = np.array(master_data)
    df = pd.DataFrame(master_np, columns=column_headers)
    print(df)