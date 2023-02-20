from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from collections import OrderedDict

import boto3
import json
from boto3.dynamodb.conditions import Key, Attr

# Import custome files/modules
import global_vars
from create_table import get_iso3

rank_id = {
    1: "spreisig_economic",
    2: "spreisig_non_economic"
}

def calc_population_density(dynamodb_res, country_name, year):
    table = dynamodb_res.Table(rank_id[2])
    pop_response = table.scan(
        AttributesToGet=["Country Name", year]
    )
    table = dynamodb_res.Table(rank_id[1])
    area_response = table.scan(
        AttributesToGet=["Area"]
    )

    ranks = {}
    for i in pop_response["Items"]:
        try:
            ranks[i["Country Name"]] = round((i[year]/area_response[j]["Area"]), 2)
            j += 1
        except:
            ranks[i["Country Name"]] = 0

    # Sorting ranks dictionary by value
    sorted_ranks = {}
    sorted_ranks = sorted(ranks.items(), key=lambda x:x[1])
    sorted_ranks_list = OrderedDict(sorted_ranks)
    # # print(ranks)
    # # print("")
    # # print(sorted_ranks)
    # print("rank: "+ str(list(sorted_ranks_list.keys()).index(country_name)))
    return int(list(sorted_ranks_list.keys()).index(country_name))

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

def get_rank(dynamodb_res, country_name, item_key, r_id):
    table = dynamodb_res.Table(rank_id[r_id])

    response = table.scan(
        AttributesToGet=["Country Name", item_key]
    )

    ranks = {}
    for i in response["Items"]:
        try:
            ranks[i["Country Name"]] = int(i[item_key])
        except:
            ranks[i["Country Name"]] = 0

    # Sorting ranks dictionary by value
    sorted_ranks = {}
    sorted_ranks = sorted(ranks.items(), key=lambda x:x[1])
    sorted_ranks_list = OrderedDict(sorted_ranks)
    # print(ranks)
    # print("")
    # print(sorted_ranks)
    # print("rank: "+ str(list(sorted_ranks_list.keys()).index(country_name)))
    return int(list(sorted_ranks_list.keys()).index(country_name))

def build_country_report(dynamodb_res, country_name):
    
    doc = SimpleDocTemplate("Report_A.pdf", pagesize=letter, rightMargin=12, leftMargin=12, topMargin=12, bottomMargin=12)

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
                    population_values.append(int(calc_population_density(dynamodb_res, country_name, value)))
                    # population_values.append("<rank>")
                    
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
        Paragraph('Population'),
        Paragraph('Table of Population, Population Density, and their respective world ranking for that year ordered by year:'),
        population_table,
        Spacer(1*cm, 1*cm),
        Paragraph('Economics\nCurrency: ' + str(get_econ_item(dynamodb_res, country_name, "Currency"))),
        Paragraph('\nTable of GDP per capita (GDPPC) from earliet year to latest year, and rank within the world for that year'),
        econ_table
    ]
    doc.build(flowables)

def build_global_report(dynamodb_res, year):

    table = dynamodb_res.Table("spreisig_economic")
    print(table.item_count)
    
    doc = SimpleDocTemplate("Report_B.pdf", pagesize=letter, rightMargin=12, leftMargin=12, topMargin=12, bottomMargin=12)

    # Population table
    data = [["Country Name", "Population", "Rank"],
            []]
    population_table = Table(data, colWidths=[167 for i in range(1,6)], rowHeights=[40 for i in range(1,3)])
    population_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                       ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                       ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
    # Area table
    data = [["Country Name", "Area (sq km)", "Rank"],
            []]
    area_table = Table(data, colWidths=[167 for i in range(1,6)], rowHeights=[40 for i in range(1,3)])
    area_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                       ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                       ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
    # Density table
    data = [["Country Name", "Density (people / sq km)", "Rank"],
            []]
    den_table = Table(data, colWidths=[167 for i in range(1,6)], rowHeights=[40 for i in range(1,3)])
    den_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                       ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                       ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))

    styles = getSampleStyleSheet()

    flowables = [
        Paragraph('Global Report', styles['Title']),
        Paragraph('Year: ' + year),
        Spacer(1*cm, 1*cm),
        Paragraph('Table of Countries ranked by population (largest to smallest)'),
        population_table,
        Spacer(1*cm, 1*cm),
        Paragraph('Table of countries ranked by area (largest to smallest)'),
        area_table,
        Spacer(1*cm, 1*cm),
        Paragraph('Table of countries ranked by density (largest to smallest)'),
        den_table
    ]
    doc.build(flowables, onFirstPage=onFirstPage)