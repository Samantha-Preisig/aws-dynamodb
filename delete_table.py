# Authour: Samantha Preisig
# File: delete_table.py (module)
# Brief: deletes a given DynamoDB table

import boto3

# Purpose: deletes given table. Table does not need to be empty
# Params:
#   - dynamodb_client: low-level interface for AWS services requests
#   - table_name: name of table to be deleted
def delete_table(dynamodb_client, table_name):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        print("Cannot delete a table that does not exist")
    else:
        print("\nDeleting " + table_name + " ...")
        dynamodb_client.delete_table(TableName=table_name)
        waiter = dynamodb_client.get_waiter("table_not_exists")
        waiter.wait(TableName=table_name)
        print("Table status: " + table_name + " deleted")