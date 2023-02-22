# Authour: Samantha Preisig
# File: delete_records.py (module)
# Brief: deletes records from DynamoDB tables listed in data/delete_records.txt

import boto3

# Import custom files/modules
import global_vars
from create_table import get_table_keys

# Purpose: deletes record idenified by the key_dict from given
# table_name
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - table_name: name of table containing record to be deleted
#   - key_dict: dictionary representation of deleted record's (table) keys
# and values
def delete(dynamodb_res, table_name, key_dict):
    table = dynamodb_res.Table(table_name)
    table.delete_item(
        Key=key_dict
    )
    return True

# Purpose: reads data/delete_records.txt line by line, parsing each line to gather
# the specific details (table name, table key(s) and their respective values) to
# identify the record to delete from table
# Please refer to README.md for details on delete_records.txt structure and usability
# Assumption: due to the scale of this assignment, delete_records.txt will never
# be a large file (to the point of causing increased time complexity), therefore each
# line of the file is stored for further parsing
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
def delete_records(dynamodb_res):
    with open(global_vars.delete_records_file, "r") as f:
        lines = f.readlines()

    line_counter = 1
    for line in lines:
        table_name = line.split(":")[0]
        table_data = line.split(":")[1]
        table_data = table_data.split(',')
        
        table_keys = {}
        data_keys = []
        for data in table_data:
            data.replace("\n", "")
            value = ""
            # If key is multiple words
            if(len(data.split()) >= 3): # Example, key = Country Name, value = Canada
                key = data.split()[0] + " " + data.split()[1] # Largest key contains 2 words with given csv tables
                for i in range(2, len(data.split())-1):
                    value += data.split()[i] + " "
                value += data.split()[-1]
            else:
                key, value = data.split()
            table_keys[key] = value
            data_keys.append(key)

        del_ret = False
        part, sort = get_table_keys(table_name)
        if(sort == ""): # Table does not have sort key
            if(part == data_keys[0]):
                del_ret = delete(dynamodb_res, table_name, table_keys)
        else:
            if(part == data_keys[0] and sort == data_keys[1]):
                del_ret = delete(dynamodb_res, table_name, table_keys)
        
        if del_ret:
            print("Record '" + line.replace('\n', '') + "' deleted")
        else:
            print("Could not delete record on line " + str(line_counter))
        line_counter += 1