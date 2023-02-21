import boto3
import json
import pandas as pd

# Import custom files/modules
import global_vars

# Initial load of data from csv-json files
def bulk_load(dynamodb_res, table_name, json_filename):
    table = dynamodb_res.Table(table_name)

    with open(json_filename, "r") as json_file:
        data = json.load(json_file)
        for key, value in data.items():
            data[key] = {key: value for key, value in data[key].items() if key}
            table.put_item(
                Item=data[key]
            )

def load(dynamodb_res, table_name, record_dict):
    table = dynamodb_res.Table(table_name)
    table.put_item(
        Item=record_dict
    )

def add_data():
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
        # else:
        #     i -= 1 # Removing addition 1 when parsing multi-word country name
        # # print(country, i+1)

        missing_info = []
        missing_info.append(data.split()[i])
        missing_info.append(data.split()[i+1])
        # for j in range(i+1, len(data.split()), 2):
        #     missing_info[data.split()[j]] = data.split()[j+1]
        # print(missing_info)

        # for key in missing_info:
            # print(pd.isna(df[key]))
            # print("HEY = "+ key)
            # if(pd.isnull())
            # # if(pd.isna(df[key])):
            # #     df[key] = missing_info[key]

        mod_df = df
        for i, row in df.iterrows():
            if pd.isnull(row[missing_info[0]]):
                mod_df = df.fillna(missing_info[1])
                # print(i, row)
                # df.loc[i, missing_info[0]] = int(missing_info[1])
        mod_df.to_csv(filename, index=False)

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
                value = data.split()[2]
            else:
                key, value = data.split()
            record_dict[key] = value
        load(dynamodb_res, table_name, record_dict)
        print("Record '" + line.replace('\n', '') + "' added")
