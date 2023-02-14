# aws-dynamodb

## TODO (personal):
- Organize project structure for personal github

## Project structure
```
A2/
|-- data/
|   |-- json/
|
|-- help/
|-- main.py
|-- cli_commands.py
|-- README.md
```
- `data/` holds `json/` directory, all csv files, `add_records.txt`, and `delete_records.txt`
    - `data/json/` holds all converted json files
    - `add_records.txt` is a file which can be edited by the user. This file contains a list of records to add to a specified table
    - `delete_records.txt` is a file which can be edited by the user. This file contains a list of records to remove from a specified table
- `help/` holds text files with information about each cli command
- `main.py` sets up global vars (shared across modules) and configuration for AWS session
- `cli_commands.py` is a module holding all cli functionality

# CLI functionality and commands

## Adding records to tables using `data/add_records.txt`
`add_records.txt` contains a list of records to be added to a specified table.
- The user must edit this file prior to using the cli command `add_records`
- The file will be read line by line, assuming each line contains **one** record with a specific configuration
- Each record listed in `add_records.txt` must be of the following order/configuration: `csv_filename: [Partition key] value, [Sort key *if it exists for the current csv_filename table] value, [Column name] value, [Column name] value, etc`
    - For example, the following will add a record to spreisig_un_shortlist table: `un_shortlist.csv: ISO3 AUS1, Official Name Australia, Country Name Australia, ISO2 AU1`
### Assumptions and limitations to adding records:
- All keys and their values must be listed (in the order of the table's columns)
### TODO
- Check what happens if add_records.txt is empty

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

## Changes made to CSV files
- Headers that were originally `Country` were changed to `Country Name` to achieve uniform table configuration
- shortlist_curpop.csv had only one column labelled `Population 1970`, with the following columns only labelled the year (`1971`, `1972`, etc). Therefore, `Population 1970` was renamed to `1970` to achieve uniform table configuration

## Assumptions/changes/notes for TA
- headers that were originally 'Country' were changed to 'Country Name' for convinience
- shortlist_curpop had a column called "Population 1970" which I changed to "1970" to match the following column pattern
- automatic bulk load is done when creating all tables from csv files
- explain project structure
- add_records.txt needs to contain case sensitive keys (to match the column headers)
- dump command only displays information for a table if it's populated