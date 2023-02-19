from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors

import boto3
import json
from boto3.dynamodb.conditions import Key, Attr

# Import custome files/modules
import global_vars
from create_table import get_iso3

def onFirstPage(canvas, document):
    canvas.drawCentredString(100, 100, '')

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

    # Gathering info for pop_table
    pop_info = []
    pop_values = []
    with open(global_vars.json_dir+"shortlist_non_economic.json", 'r') as json_file:
        data_dict = json.load(json_file)

        for key in data_dict:
            for value in data_dict[key]:
                pop_values = []
                if(key == country_name and value.isnumeric()):
                    pop_values.append(value)
                    pop = get_non_econ_item(dynamodb_res, country_name, value)
                    pop_values.append(str(pop))
                    # print(str(get_non_econ_item(dynamodb_res, country_name, value)))
                    pop_values.append("<rank>")
                    density = round(pop/area, 2)
                    pop_values.append(str(density))
                    pop_values.append("<rank>")
                    pop_info.append(pop_values)
    # print(pop_info)
    # print(len(pop_info))

    # Population table
    data = pop_info
    pop_table = Table(data, colWidths=[100 for i in range(1,6)], rowHeights=[20 for i in range(1,len(pop_info)+1)])
    pop_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                       ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                       ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
    # Economic table
    data = [["Year", "GDPPC", "Rank"],
            []]
    econ_table = Table(data, colWidths=[167 for i in range(1,6)], rowHeights=[40 for i in range(1,3)])
    econ_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                       ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                       ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))

    styles = getSampleStyleSheet()

    flowables = [
        Paragraph('Name of Country', styles['Title']),
        Paragraph('[Official Name: ' + str(get_non_econ_item(dynamodb_res, country_name, "Official Name")) + ']'),
        Spacer(1*cm, 1*cm),
        general_table,
        Spacer(1*cm, 1*cm),
        Paragraph('Text after general_table\nPopulation table:'),
        pop_table,
        Spacer(1*cm, 1*cm),
        Paragraph('Text after pop_table\nEconomics table:'),
        Paragraph('Currency:'),
        econ_table
    ]
    doc.build(flowables, onFirstPage=onFirstPage)

def build_global_report(dynamodb_res, year):
    
    doc = SimpleDocTemplate("Report_B.pdf", pagesize=letter, rightMargin=12, leftMargin=12, topMargin=12, bottomMargin=12)

    # Population table
    data = [["Country Name", "Population", "Rank"],
            []]
    pop_table = Table(data, colWidths=[167 for i in range(1,6)], rowHeights=[40 for i in range(1,3)])
    pop_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
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
        Paragraph('Year:'),
        Spacer(1*cm, 1*cm),
        Paragraph('Table of Countries ranked by population (largest to smallest)'),
        pop_table,
        Spacer(1*cm, 1*cm),
        Paragraph('Table of countries ranked by area (largest to smallest)'),
        area_table,
        Spacer(1*cm, 1*cm),
        Paragraph('Table of countries ranked by density (largest to smallest)'),
        den_table
    ]
    doc.build(flowables, onFirstPage=onFirstPage)