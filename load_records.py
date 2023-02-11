import boto3
import json

dict_tables = {
    "shortlist_area.csv": {"table_name": "spreisig_shortlist_area", "key_columns": ["ISO3", "Area"]},
    "shortlist_capitals.csv": {"table_name": "spreisig_shortlist_capitals", "key_columns": ["ISO3", "Capital"]},
    "shortlist_curpop.csv": {"table_name": "spreisig_shortlist_curpop", "key_columns": ["\ufeffCountry Name"]}, # "shortlist_curpop.csv": {"table_name": "spreisig_shortlist_curpop", "key_columns": ["Currency"]},
    "shortlist_gdppc.csv": {"table_name": "spreisig_shortlist_gdppc", "key_columns": ["\ufeffCountry Name"]},
    "shortlist_languages.csv": {"table_name": "spreisig_shortlist_languages", "key_columns": ["ISO3"]},
    "un_shortlist.csv": {"table_name": "spreisig_un_shortlist", "key_columns": ["ISO3", "Official Name"]}
}

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

# def add_record():

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
