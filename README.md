# aws-dynamodb

## About
### What is Amazon DynamoDB?
DynamoDB is a fully managed (hosted) NoSQL database offered by Amazon Web Services (AWS)

Given a set of csv files, two tables are created and populated based on economic and non-economic data in DynamoDB. Tables can be manipulated by adding missing information (individual cells), adding/deleting records, displaying data of specified table, and querying/scanning tables for report production. Two reports can be built from these tables; Report_A.pdf outlines a country's information given a specified country and Report_B.pdf outlines global information for specified year\
To allow the user to perform multiple manipulations during runtime, I've built a CLI program\
The list of csv files include:
- `un_shortlist.csv`: contains ISO3, Country Name, Official Name, and ISO2 for 40 countries
- `shortlist_area.csv`: contains ISO3, Country Name, and Area for 40 countries
- `shortlist_curpop.csv`: contains Country Name, Currency, and population for year 1970, 1971,..., 2019 for 40 countries
- `shortlist_gdppc.csv`: contains Country Name and GDP per capita for year 1970, 1971,..., 2019 for 40 countries
- `shortlist_languages.csv`: contains ISO3, Country Name, and (list of) Languages for 40 countries
- `shortlist_capitals.csv`: contains ISO3, Country Name, and Capital for 40 countries

## Project structure
```
A2/
|-- data/
|   |-- add_records.txt
|   |-- delete_records.txt
|   |-- missing_info.txt
|   |-- README.md
|   |-- XXX.csv
|   |-- json/
|
|-- help/
|   |-- default.txt
|   |-- XXX.txt
|
|-- main.py
|-- cli_commands.py
|-- global_vars.py
|-- create_table.py
|-- delete_table.py
|-- load_records.py
|-- delete_records.py
|-- dump.py
|-- build_reports.py
|-- Requirements.txt
|-- README.md
```
- `data/` holds `json/` directory, all csv files, `add_records.txt`, `delete_records.txt`, and `missing_info.txt`
    - `data/json/` holds all converted json files
    - `add_records.txt` is a file which can be edited by the user. This file contains a list of records to add to a specified table
    - `delete_records.txt` is a file which can be edited by the user. This file contains a list of records to remove from a specified table
    - `missing_info.txt` is a file which can be edited by the user. This file contains a list of missing information to add to specified csv files
- `help/` holds text files with information about each cli command
- `main.py` sets up global vars (shared across modules) and configuration for AWS session
- `cli_commands.py` is a module holding all cli functionality

# CLI functionality and commands
## Adding missing information to csv files using `data/missing_info.txt`
`missing_info.txt` contains a list of missing information (for specific cells) for a given csv file
- The user must edit this file prior to using the cli command `add_data`
- The file will be read line by line, assuming each line contains **one** cell of missing information with a specific configuration
- The specific configuration for each line in `missing_info.txt` is the following: `csv_file: country_name, key value`. The csv filename (with ':') and country name is required in order to identify the csv row. The key and value represent the column and value to populate the cell, respectively
    - For example, `shortlist_curpop.csv: Australia, 2019 25203198` adds the missing population value for Australia under column '2019' with the value 25203198
### TODO
- Check what happens if missing_info.txt is empty
- Error handling for items in each line that DNE

## Adding records to tables using `data/add_records.txt`
`add_records.txt` contains a list of records to be added to a specified table
- The user must edit this file prior to using the cli command `add_records`
- The file will be read line by line, assuming each line contains **one** record with a specific configuration
- Each record listed in `add_records.txt` must be of the following order/configuration: `csv_filename: [Partition key] value, [Sort key *if it exists for the current csv_filename table] value, [Column name] value, [Column name] value, etc`
    - For example, the following will add a record to spreisig_un_shortlist table: `un_shortlist.csv: ISO3 AUS1, Official Name Australia, Country Name Australia, ISO2 AU1`
### Assumptions and limitations to adding records:
- All keys and their values must be listed (in the order of the table's columns)
### TODO
- Check what happens if add_records.txt is empty
- Error handling for items in each line that DNE

## Removing records from tables using `data/delete_records.txt`
`delete_records.txt` contains a list of records to be removed from a specified table.
- The user must edit this file prior to using the cli command `delete_records`
- The file will be read line by line, assuming each line contains **one** record with a specific configuration
- Each record listed in `delete_records.txt` must be of the following order/configuration: `csv_filename: [Partition key] value, [Sort key *if it exists for the current csv_filename table] value`. *Similar to add_records.txt, however, only the table keys (and their corresponding values) are listed*
    - For example, the following will delete a record from spreisig_un_shortlist table: `un_shortlist.csv: ISO3 AUS1, Official Name Australia`
### Assumptions and limitations to removing records:
- Removes records based only on the partition and sort keys with their corresponding values
### TODO
- Check what happens if delete_records.txt is empty
- Error handling for items in each line that DNE

## Changes made to CSV files
- Headers that were originally `Country` were changed to `Country Name` to achieve uniform table configuration
- shortlist_curpop.csv had only one column labelled `Population 1970`, with the following columns only labelled the year (`1971`, `1972`, etc). Therefore, `Population 1970` was renamed to `1970` to achieve uniform table configuration

## Assumptions/Limitations
### General
- Only tables required for manipulation contain economic and non-economic data
- An automatic bulk load is performed upon initial creation of tables (on cli-startup)
### Data organization
- Economic data consists of the following:
    - GDPPC (shortlist_gdppc.json)
    - Currency (shortlist_curpop.json)
- Non-economic data consists of the following:
    - Area (shortlist_area.json)
    - Capital (shortlist_capitals.json)
    - Population (shortlist_curpop.json)
    - Languages (shortlist_languages.json)
    - ISO2/Official Name (un_shortlist.json)
### Adding missing information to csv files
- Adding new languages to populated language cells in csv files (via `missing_info.txt`) raises error. The following missing_info lines will not work:
```
shortlist_languages.csv: Comoros, Languages French
shortlist_languages.csv: 'Cook Islands', Languages English
```
### Adding new records to table
- Each line in `add_records.txt` MUST follow a specified structure (outlined above)
- [TODO - easy handle] Case sensitive: column headers (such as 'Country Name' and 'Area') need to be capitalized in order to detect table headers
