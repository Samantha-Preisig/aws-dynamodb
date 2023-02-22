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
- Each record listed in `add_records.txt` must be of the following order/configuration: `table_name: [Partition key] value, [Sort key *if it exists*] value, [Column name] value, [Column name] value, etc`
    - For example, the following will add a record to spreisig_economic: `spreisig_economic: Country Name Australia1, 1970 12345, Currency AUD`. This record adds data to 'Country Name', '1970', and 'Currency' columns, with all other columns empty in this record
### TODO
- Check what happens if add_records.txt is empty
- Error handling for items in each line that DNE

## Removing records from tables using `data/delete_records.txt`
`delete_records.txt` contains a list of records to be removed from a specified table.
- The user must edit this file prior to using the cli command `delete_records`
- The file will be read line by line, assuming each line contains **one** record with a specific configuration
- Each record listed in `delete_records.txt` must be of the following order/configuration: `table_name: [Partition key] value, [Sort key *if it exists*] value`. *Similar to add_records.txt, however, only the table keys (and their corresponding values) are listed*
    - For example, the following will delete a record from spreisig_economic: `spreisig_economic: Country Name Australia`
### TODO
- Check what happens if delete_records.txt is empty
- Error handling for items in each line that DNE

## Assumptions/Limitations
### Adding missing information to csv files
- Adding new languages to populated language cells in csv files (via `missing_info.txt`) raises error. The following missing_info lines will not work:
```
shortlist_languages.csv: Comoros, Languages French
shortlist_languages.csv: 'Cook Islands', Languages English
```
### Adding new records to table
- Each line in `add_records.txt` MUST follow a specified structure (outlined above)
- [TODO - easy handle] Case sensitive: column headers (such as 'Country Name' and 'Area') need to be capitalized in order to detect table headers
- `[Check]` All keys and their values must be listed (in the order of the table's columns)