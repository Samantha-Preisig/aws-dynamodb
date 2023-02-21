import boto3

def delete_table(dynamodb_client, table_name):
    existing_tables = dynamodb_client.list_tables()['TableNames']
    if table_name not in existing_tables:
        print("Cannot delete a table that does not exist")
    else:
        print("Deleting " + table_name + " ...")
        dynamodb_client.delete_table(TableName=table_name)
        waiter = dynamodb_client.get_waiter("table_not_exists")
        waiter.wait(TableName=table_name)
        print("Table status: " + table_name + " deleted")