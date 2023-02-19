from reportlab.lib.enums import TA_JUSTIFY, TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors

def onFirstPage(canvas, document):
    canvas.drawCentredString(100, 100, 'Text drawn with onFirstPage')

def build_country_report(country_name):
    
    doc = SimpleDocTemplate("Report_A.pdf", pagesize=letter, rightMargin=12, leftMargin=12, topMargin=12, bottomMargin=12)

    # Top table (area, languages, capital)
    data = [["Area:"],
            ["Official/National Languages:\nCapital City:"]]
    general_table = Table(data, colWidths=200, rowHeights=[40 for i in range(1,3)])
    general_table.setStyle(TableStyle([("BOX", (0, 0), (-1, -1), 1, colors.black),
                                       ("GRID", (0, 0), (-1, -1), 1, colors.black),
                                       ("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
    # Population table
    data = [["Year", "Population", "Rank", "Population Density", "Rank"],
            []]
    pop_table = Table(data, colWidths=[100 for i in range(1,6)], rowHeights=[40 for i in range(1,3)])
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
        Paragraph('Official Name'),
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

def build_global_report():
    
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