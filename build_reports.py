from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from collections import OrderedDict
from boto3.dynamodb.conditions import Key, Attr
import boto3
import json
import numpy as np

# Import custome files/modules
import global_vars
from create_table import get_iso3

# Used in get_rank() to identify which table item_key belongs to
rank_id = {
    1: "spreisig_economic",
    2: "spreisig_non_economic"
}

# Purpose: queries the non-economic table ("spreisig_non_economic") to retrieve
# the value from item_key for the given country_name
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - country_name: the country holding requested data
#   - item_key: the key to the value requested (such as "Area", "Languages", "Capital")
# Returns: the value for item_key, "-" otherwise
def get_non_econ_item(dynamodb_res, country_name, item_key):
    table = dynamodb_res.Table("spreisig_non_economic")

    response = table.query(
        KeyConditionExpression=Key("ISO3").eq(get_iso3(country_name))
    )
    for i in response["Items"]:
        try:
            return i[item_key]
        except:
            return "-"

# Purpose: queries the economic table ("spreisig_economic") to retrieve
# the value from item_key for the given country_name
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - country_name: the country holding requested data
#   - item_key: the key to the value requested (such as "Currency" or years for GDPPC values)
# Returns: the value for item_key, "-" otherwise
def get_econ_item(dynamodb_res, country_name, item_key):
    table = dynamodb_res.Table("spreisig_economic")

    response = table.query(
        KeyConditionExpression=Key("Country Name").eq(country_name)
    )
    for i in response["Items"]:
        try:
            return i[item_key]
        except:
            return "-"

# Purpose: calculates population density for all countries with the given year and
# returns the population-density rank of the given country_name
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - country_name: the country to return population-density rank
#   - year: population densities are calculated for the given year
# Returns: int representing the country_name's population-density rank
def calc_population_density_rank(dynamodb_res, country_name, year):
    # Scanning non-economic table for population and area data for the given country_name
    table = dynamodb_res.Table(rank_id[2])
    response = table.scan(
        AttributesToGet=["Country Name", year, "Area"]
    )

    densities = {}
    for i in range(0, len(response["Items"])):
        item = response["Items"][i]
        densities[item["Country Name"]] = round((int(item[year])/item["Area"]), 2)*-1

    # Sorting densities dictionary by value (value = density)
    sorted_densities = {}
    sorted_densities = sorted(densities.items(), key=lambda x:x[1])
    sorted_densities_list = OrderedDict(sorted_densities)

    # Returning the index of the given country_name within the sorted density list (sorted_densities_list)
    return int(list(sorted_densities_list.keys()).index(country_name))+1 # Adding 1 as index counts from 0

# Purpose: scans a table (identified by r_id) and returns the item_key rank
# for the given country_name
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - country_name: the country the returned rank value belongs to
#   - item_key: the key to the value requested (such as "Area", or years for GDPPC/population values)
#   - r_id: identifies which table to scan using this module's global dictionary "rank_id"
# Returns: int representing the country_name's item_key rank
def get_rank(dynamodb_res, country_name, item_key, r_id):
    # Scanning economic (r_id=1) or non-economic (r_id=2) table for item_key
    table = dynamodb_res.Table(rank_id[r_id])

    response = table.scan(
        AttributesToGet=["Country Name", item_key]
    )

    ranks = {}
    for i in response["Items"]:
        try:
            ranks[i["Country Name"]] = int(i[item_key])*-1 # Multiply by -1 to reverse sort order
        except:
            ranks[i["Country Name"]] = 0

    # Sorting ranks dictionary by value (value = density)
    sorted_ranks = {}
    sorted_ranks = sorted(ranks.items(), key=lambda x:x[1])
    sorted_ranks_list = OrderedDict(sorted_ranks)

    # Returning the index of the given country_name within the sorted density list (sorted_ranks_list)
    return int(list(sorted_ranks_list.keys()).index(country_name))+1 # Adding 1 as index counts from 0

# Purpose: builds Report_A.pdf containing tables of information about a specified
# country using ReportLab
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - country_name: the country the report is based on
def build_country_report(dynamodb_res, country_name):
    
    doc = SimpleDocTemplate("Report_A.pdf", pagesize=letter, rightMargin=50, leftMargin=50, topMargin=50, bottomMargin=50)

    # Top table (area, languages, capital)
    area = get_non_econ_item(dynamodb_res, country_name, "Area")
    data = [["Area: " + str(area)],
            ["Official/National Languages: " + str(get_non_econ_item(dynamodb_res, country_name, "Languages")) + "\nCapital City: " + str(get_non_econ_item(dynamodb_res, country_name, "Capital"))]]
    general_table = Table(data, colWidths=200, rowHeights=[40 for i in range(1,3)])
    general_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                       ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                       ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))

    # Gathering info for population_table
    population_info = []
    population_info.append(["Year", "Population", "Rank", "Population Density\n(people/sq km)", "Rank"]) # Appending header row
    with open(global_vars.json_dir+"shortlist_non_economic.json", 'r') as json_file:
        data_dict = json.load(json_file)

        for key in data_dict:
            for value in data_dict[key]:
                population_values = []
                if(key == country_name and value.isnumeric()):
                    population_values.append(value) # Year
                    population = get_non_econ_item(dynamodb_res, country_name, value) # Population during that year
                    population_values.append(str(population))
                    
                    # Getting population rank during that year
                    population_values.append(int(get_rank(dynamodb_res, country_name, value, 2)))
                    
                    # Calculating and storing density
                    density = round(population/area, 2)
                    population_values.append(str(density))
                    
                    # Getting population-density rank during that year
                    population_values.append(int(calc_population_density_rank(dynamodb_res, country_name, value)))
                    
                    # Appening population_values list to master population list
                    population_info.append(population_values)

    # Population table
    data = population_info
    population_table = Table(data, colWidths=[100 for i in range(1,6)], rowHeights=[25 for i in range(1,len(population_info)+1)])
    population_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                          ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                          ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
    
    # Gathering info for economic_table
    econ_info = []
    econ_info.append(["Year", "GDPPC", "Rank"]) # Appending header row
    with open(global_vars.json_dir+"shortlist_economic.json", 'r') as json_file:
        data_dict = json.load(json_file)

        for key in data_dict:
            for value in data_dict[key]:
                econ_values = []
                if(key == country_name and value.isnumeric()):
                    econ_values.append(value) # Year
                    gdppc = get_econ_item(dynamodb_res, country_name, value) # Population during that year
                    econ_values.append(str(gdppc))
                    
                    # Getting GDPPC rank during that year
                    econ_values.append(int(get_rank(dynamodb_res, country_name, value, 1)))
                    
                    # Appening population_values list to master population list
                    econ_info.append(econ_values)

    # Economic table
    data = econ_info
    econ_table = Table(data, colWidths=[167 for i in range(1,6)], rowHeights=[25 for i in range(1,len(econ_info)+1)])
    econ_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                    ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))

    # Styles for flowables
    styles = getSampleStyleSheet()
    centered_header = ParagraphStyle('official_name',
                                      parent=styles['Heading3'],
                                      alignment=TA_CENTER)

    flowables = [
        Paragraph(country_name, styles['Title']),
        Paragraph('[Official Name: ' + str(get_non_econ_item(dynamodb_res, country_name, "Official Name")) + ']', centered_header),
        Spacer(1*cm, 1*cm),
        general_table,
        Spacer(1*cm, 1*cm),
        Paragraph('Population', styles['Heading2']),
        Paragraph('Table of Population, Population Density, and their respective world ranking for that year ordered by year:'),
        Spacer(0.3*cm, 0.3*cm),
        population_table,
        Spacer(1*cm, 1*cm),
        Paragraph('Economics', styles['Heading2']),
        Paragraph('Currency: ' + str(get_econ_item(dynamodb_res, country_name, "Currency")), styles['Heading3']),
        Paragraph('\nTable of GDP per capita (GDPPC) from earliet year to latest year, and rank within the world for that year'),
        Spacer(0.3*cm, 0.3*cm),
        econ_table
    ]
    doc.build(flowables)

# Purpose: builds Report_B.pdf containing tables of information about a specified
# year using ReportLab
# Params:
#   - dynamodb_res: high-level abstraction for AWS services requests
#   - year: the year the report is based on
def build_global_report(dynamodb_res, year):
    
    doc = SimpleDocTemplate("Report_B.pdf", pagesize=letter, rightMargin=50, leftMargin=50, topMargin=50, bottomMargin=50)

    # Gathering info for population_table
    population_info = []
    area_info = []
    density_info = []
    with open(global_vars.json_dir+"shortlist_non_economic.json", 'r') as json_file:
        data_dict = json.load(json_file)

        for key in data_dict:
            pop_area = []
            for value in data_dict[key]:
                population_values = []
                area_values = []
                density_values = []
                
                # Gathering population information
                if(value == year):
                    population_values.append(key) # key = Country Name
                    population = (data_dict[key][value]) # Population for that year
                    population_values.append(population)

                    # Getting population rank during that year
                    population_values.append(str(get_rank(dynamodb_res, key, value, 2)))
                    
                    # Appening population_values list to master population list
                    population_info.append(population_values)
                
                # Gathering area information
                if(value == "Area"):
                    area_values.append(key) # key = Country Name
                    area = (data_dict[key][value])
                    area_values.append(area)

                    # Getting population rank during that year
                    area_values.append(str(get_rank(dynamodb_res, key, value, 2)))
                    
                    # Appening population_values list to master population list
                    area_info.append(area_values)
                
                # Gathering population density information
                if population_values:
                    pop_area.append(population_values[1])
                if area_values:
                    pop_area.append(area_values[1])
                
                if(len(pop_area) == 2):
                    density_values.append(key) # key = Country Name
                    
                    # Calculating and storing density
                    density = round(pop_area[1]/pop_area[0], 2)
                    density_values.append(str(density))
                    
                    # Getting population-density rank during that year
                    density_values.append(int(calc_population_density_rank(dynamodb_res, key, year)))

                    # Appening density_values list to master density list
                    density_info.append(density_values)
                    pop_area = [] # Clearing pop_area for next country
    
    # Population table
    population_info = sorted(population_info, key=lambda row: int(row[2])) # Sorting population_info by rank
    header_row = np.array(["Country Name", "Population", "Rank"])
    data = np.array(np.vstack([header_row, np.array(population_info)])).tolist()

    population_table = Table(data, colWidths=[167 for i in range(1,len(data[0])+1)], rowHeights=[25 for i in range(1,len(data)+1)])
    population_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                          ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                          ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
    
    # Area table
    area_info = sorted(area_info, key=lambda row: int(row[2])) # Sorting area_info by rank
    header_row = np.array(["Country Name", "Area (sq km)", "Rank"])
    data = np.array(np.vstack([header_row, np.array(area_info)])).tolist()

    area_table = Table(data, colWidths=[167 for i in range(1,len(data[0])+1)], rowHeights=[25 for i in range(1,len(data)+1)])
    area_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                    ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
    
    # Density table
    density_info = sorted(density_info, key=lambda row: int(row[2])) # Sorting density_info by rank
    header_row = np.array(["Country Name", "Density (people / sq km)", "Rank"])
    data = np.array(np.vstack([header_row, np.array(density_info)])).tolist()
    
    den_table = Table(data, colWidths=[167 for i in range(1,len(data[0])+1)], rowHeights=[25 for i in range(1,len(data)+1)])
    den_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                   ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                   ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))

    styles = getSampleStyleSheet()

    flowables = [
        Paragraph('Global Report', styles['Title']),
        Paragraph('Year: ' + year),
        Spacer(1*cm, 1*cm),
        Paragraph('Table of Countries Ranked by Population (largest to smallest)'),
        population_table,
        Spacer(1*cm, 1*cm),
        Paragraph('Table of Countries Ranked by Area (largest to smallest)'),
        area_table,
        Spacer(1*cm, 1*cm),
        Paragraph('Table of Countries Ranked by Density (largest to smallest)'),
        den_table
    ]
    doc.build(flowables)