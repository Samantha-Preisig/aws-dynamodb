import boto3

# Import custom files/modules
import global_vars
from create_table import get_table_keys

def delete(dynamodb_res, table_name, key_dict):
    table = dynamodb_res.Table(table_name)
    table.delete_item(
        Key=key_dict
    )
    return True

def delete_record(dynamodb_res):
    with open(global_vars.delete_records_file, "r") as f:
        lines = f.readlines()

    table_name = ""
    line_counter = 1
    for line in lines:
        filename = line.split(':')[0]
        table_name = "spreisig_"+(filename.replace('.csv', ''))
        table_data = line.split(':')[1]
        table_data = table_data.split(',')
        
        table_keys = {}
        data_keys = []
        for data in table_data:
            value = ""
            if(len(data.split()) >= 3): # Example, key = Country Name, value = Canada
                key = data.split()[0] + " " + data.split()[1] # Largest key contains 2 words with given csv tables
                # if(key == "Country Name" and not table_keys):
                #     key = "\ufeff"+key
                for i in range(2, len(data.split())-1):
                    value += data.split()[i] + " "
                value += data.split()[-1]
            else:
                key, value = data.split()
            table_keys[key] = value
            data_keys.append(key)

        # print(table_keys)
        del_ret = False
        part, sort = get_table_keys(table_name)
        if(sort == ""):
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