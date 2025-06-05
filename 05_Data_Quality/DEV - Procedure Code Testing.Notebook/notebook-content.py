# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe",
# META       "default_lakehouse_name": "Bronze_Lakehouse_DEV",
# META       "default_lakehouse_workspace_id": "4748fbe5-9b18-4aac-9d74-f79c39ff81db",
# META       "known_lakehouses": [
# META         {
# META           "id": "b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

!pip install pymupdf

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import fitz  # PyMuPDF

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdf_path = "abfss://4748fbe5-9b18-4aac-9d74-f79c39ff81db@onelake.dfs.fabric.microsoft.com/b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe/Files/hc/dental_grids/2024_Grids/GPSP/ENGLISH_Grids/cdcp-ab-gpsp-benefit-grid-2024-e.pdf"
pdf_raw = spark.read.format("binaryFile").load(pdf_path)

# Extract raw binary content
pdf_bytes = pdf_raw.select("content").collect()[0][0]  # Get raw byte data

# Open the PDF using PyMuPDF (fitz)
doc = fitz.open(stream=pdf_bytes, filetype="pdf")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

max_pages = len(doc)  # Set your desired limit here

texts_tuples = []

for i, page in enumerate(doc):
    if i >= max_pages:
        break
    blocks = page.get_text("dict")["blocks"]

    for number in range(0,len(blocks)):
        try:
            lines = blocks[number]['lines']
            for line in range(0,len(lines)):
                spans = lines[line]['spans']
                for span in range(0,len(spans)):
                    size = spans[span]['size']
                    text = spans[span]['text']
                    texts_tuples.append((size, text))
                    print(f'{size} : {text}')
        except:
            continue

    print(f"Processing page {i + 1}")

# Filter the list
filtered_texts_tuples = [
    (size, text) for size, text in texts_tuples
    if not (
        (size == 12.0 and len(text.strip()) <= 4) or 
        ("unclassified" in text.strip().lower()) or 
        (round(size, 2) == 14.04 and (text is None or text.strip() == "")) or 
        (round(size, 2) == 15.96 and (text is None or text.strip() == "")) or 
        (round(size, 2) == 18.0 and (text is None or text.strip() == "")) 
    )
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_texts_tuples

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(texts_tuples)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import re

# Initialize DataFrame
df = pd.DataFrame(columns=['Province', 'Association', 'Schedule', 'Benefit_Category', 'Class', 'Subclass', 'Limitations', 'Procedure_Code'])

# State tracking
province_set = False
association_set = False
schedule_set = False
subcategory_set = False
service_set = False

# Row index
row = -1

# Persistent values
province = association = schedule = category = subcategory = service = None
limitations = []
procedure_codes = []

# Patterns
subcategory_pattern = re.compile(r'^\d+\.\d+\s+.*')
procedure_code_pattern = re.compile(r'^\d{5}$')

for size, text in filtered_texts_tuples:
    size = round(size, 2)

    if size == 18.0 and not province_set:
        province = text
        province_set = True

    elif size == 15.96:
        if 'Schedule' in text:
            schedule = text
            schedule_set = True
        else:
            association = text
            association_set = True

    elif size == 14.04:
        category = text
        subcategory = None
        service = None
        subcategory_set = False
        service_set = False

    elif size == 12.0:
        if row >= 0:
            if limitations:
                df.at[row, 'Limitations'] = ' '.join(limitations).strip()
                limitations = []
            if procedure_codes:
                df.at[row, 'Procedure_Code'] = ', '.join(procedure_codes)
                procedure_codes = []

        row += 1
        df.at[row, 'Province'] = province
        df.at[row, 'Association'] = association
        df.at[row, 'Schedule'] = schedule
        df.at[row, 'Benefit_Category'] = category

        if subcategory_pattern.match(text):
            df.at[row, 'Class'] = text
            subcategory = text
            subcategory_set = True
            service_set = False
        else:
            df.at[row, 'Subclass'] = text
            service = text
            service_set = True
            if subcategory_set:
                df.at[row, 'Class'] = subcategory

    elif size == 11.04 and (subcategory_set or service_set) and text.strip() and text.strip().upper() != 'NULL':
        limitations.append(text)

    elif size == 9.0 and service_set and procedure_code_pattern.match(text.strip()):
        procedure_codes.append(text.strip())

# Assign any remaining limitations and procedure codes to the last row
if row >= 0:
    if limitations:
        df.at[row, 'Limitations'] = ' '.join(limitations).strip()
    if procedure_codes:
        df.at[row, 'Procedure_Code'] = ', '.join(procedure_codes)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Importing PDF

# CELL ********************

import fitz

def load_pdf_by_year_and_province(year: int, province: str):
    # Construct the file path dynamically using the year and province
    lh_path = "abfss://4748fbe5-9b18-4aac-9d74-f79c39ff81db@onelake.dfs.fabric.microsoft.com/b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe/Files/hc/dental_grids"
    
    path_dict = {
        #2024 : f"{lh_path}/{year}_Grids/DH/ENGLISH_Grids/cdcp-{province}-dh-benefit-grid-{year}-e.pdf",
        #2025 : f"{lh_path}/{year}_Grids/DH/ENGLISH_Grids/cdcp-{province}-dh-benefit-grid-{year}-e_a11y.pdf"
        2024 : f"{lh_path}/{year}_Grids/DH/FRENCH_Grids/cdcp-{province}-hd-benefit-grid-{year}-f.pdf",
        2025 : f"{lh_path}/{year}_Grids/DH/FRENCH_Grids/cdcp-{province}-hd-benefit-grid-{year}-f_a11y.pdf"
    }
    
    pdf_path = path_dict[year]
    
    # Load the PDF as binary using Spark
    pdf_raw = spark.read.format("binaryFile").load(pdf_path)
    
    # Extract raw binary content
    pdf_bytes = pdf_raw.select("content").collect()[0][0]
    
    # Open the PDF using PyMuPDF (fitz)
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    
    return doc

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Function to Breakdown PDF Lines to List

# CELL ********************

def extract_filtered_texts(doc, max_pages=None):
    """
    Extracts and filters text spans from a document.

    Parameters:
        doc: The document object (e.g., from PyMuPDF or similar).
        max_pages: Optional maximum number of pages to process.

    Returns:
        A list of (size, text) tuples, filtered based on specific rules.
    """
    if max_pages is None:
        max_pages = len(doc)

    texts_tuples = []

    for i, page in enumerate(doc):
        if i >= max_pages:
            break

        blocks = page.get_text("dict")["blocks"]

        for block in blocks:
            try:
                for line in block['lines']:
                    for span in line['spans']:
                        size = span['size']
                        text = span['text']
                        texts_tuples.append((size, text))
                        #print(f'{size} : {text}')
            except KeyError:
                continue

        #print(f"Processing page {i + 1}")

    # Filter the list
    filtered_texts_tuples = [
        (size, text) for size, text in texts_tuples
        if not (
            (size == 12.0 and len(text.strip()) <= 4) or 
            ("unclassified" in text.strip().lower()) or 
            (round(size, 2) == 14.04 and (text is None or text.strip() == "")) or 
            (round(size, 2) == 15.96 and (text is None or text.strip() == "")) or 
            (round(size, 2) == 18.0 and (text is None or text.strip() == "")) 
        )
    ]

    return filtered_texts_tuples

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Function to Parse PDF

# CELL ********************

import pandas as pd
import re

def parse_filtered_texts(filtered_texts_tuples, association):
    df = pd.DataFrame(columns=['Province', 'Association', 'Schedule', 'Benefit_Category', 'Class', 'Subclass', 'Limitations', 'Procedure_Code'])

    condition_mapping ={ 
        'DH' : {
                    'province': 18.0,
                    'association_schedule': 15.96,
                    'benefit_category': 14.04,
                    'class_service': 12.0,
                    'limitations': 11.04,
                    'procedure_code': 9.0
                },
        'DD' : {
                    'province': 18.0,
                    'association_schedule': 15.96,
                    'benefit_category': 14.04,
                    'class_service': 12.0,
                    'limitations': 11.04,
                    'procedure_code': 9.0
                },
        'GPSP' : {
                    'province': 18.0,
                    'association_schedule': 15.96,
                    'benefit_category': 14.04,
                    'class_service': 12.0,
                    'limitations': 11.04,
                    'procedure_code': 9.0
                },
        'OMFS' : {
                    'province': 18.0,
                    'association_schedule': 15.96,
                    'benefit_category': 14.04,
                    'class_service': 12.0,
                    'limitations': 11.04,
                    'procedure_code': 9.0
                }
                    } 

    size_conditions = condition_mapping[association]

    province_set = association_set = schedule_set = classcategory_set = subclass_set = False
    row = -1

    province = association = schedule = benefit_category = classcategory = subclass = None
    limitations = []
    procedure_codes = []

    class_pattern = re.compile(r'^\d+\.\d+\s+.*')
    procedure_code_pattern = re.compile(r'^\d{5}$')

    for size, text in filtered_texts_tuples:
        size = round(size, 2)

        if size == size_conditions['province'] and not province_set:
            province = text
            province_set = True

        elif size == size_conditions['association_schedule']:
            if 'Schedule' in text:
                schedule = text
                schedule_set = True
            else:
                association = text
                association_set = True

        elif size == size_conditions['benefit_category']:
            benefit_category = text
            classcategory = subclass = None
            classcategory_set = subclass_set = False

        elif size == size_conditions['class_service']:
            if row >= 0:
                if limitations:
                    df.at[row, 'Limitations'] = ' '.join(limitations).strip()
                    limitations = []
                if procedure_codes:
                    df.at[row, 'Procedure_Code'] = ', '.join(procedure_codes)
                    procedure_codes = []

            row += 1
            df.at[row, 'Province'] = province
            df.at[row, 'Association'] = association
            df.at[row, 'Schedule'] = schedule
            df.at[row, 'Benefit_Category'] = benefit_category

            if class_pattern.match(text):
                df.at[row, 'Class'] = text
                classcategory = text
                classcategory_set = True
                subclass_set = False
            else:
                df.at[row, 'Subclass'] = text
                subclass = text
                subclass_set = True
                if classcategory_set:
                    df.at[row, 'Class'] = classcategory

        elif size == size_conditions['limitations'] and (classcategory_set or subclass_set) and text.strip() and text.strip().upper() != 'NULL':
            limitations.append(text)

        elif size == size_conditions['procedure_code'] and subclass_set and procedure_code_pattern.match(text.strip()):
            procedure_codes.append(text.strip())

    if row >= 0:
        if limitations:
            df.at[row, 'Limitations'] = ' '.join(limitations).strip()
        if procedure_codes:
            df.at[row, 'Procedure_Code'] = ', '.join(procedure_codes)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Using Functions

# CELL ********************

doc = load_pdf_by_year_and_province(2025,"pe")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_texts_tuples = extract_filtered_texts(doc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_texts_tuples

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

category_texts = [(size, text) for size, text in filtered_texts_tuples if round(size, 2) == 15.96]
category_texts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

size_conditions = {
    'province': 18.0,
    'association_schedule': 15.96,
    'benefit_category': 14.04,
    'class_service': 12.0,
    'limitations': 11.04,
    'procedure_code': 9.0
}

df = parse_filtered_texts(filtered_texts_tuples, 'DH')
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# Define the years and provinces to iterate over
years = [2024, 2025]
provinces = ["ab", "bc", "mb", "nb", "nl", "ns", "nt", "nu", "on", "pe", "qc", "sk", "yk"]

# Define size conditions
size_conditions = {
    'province': 18.0,
    'association_schedule': 15.96,
    'benefit_category': 14.04,
    'class_service': 12.0,
    'limitations': 11.04,
    'procedure_code': 9.0
}

# Initialize an empty list to collect DataFrames
all_dfs = []

# Loop through years and provinces
for year in years:
    for province in provinces:
        try:
            doc = load_pdf_by_year_and_province(year, province)
            filtered_texts_tuples = extract_filtered_texts(doc)
            df = parse_filtered_texts(filtered_texts_tuples, size_conditions)
            df["year"] = year
            df["province"] = province.upper()
            all_dfs.append(df)
        except Exception as e:
            print(f"Error processing {year} - {province.upper()}: {e}")
            continue

# Concatenate all DataFrames
final_df = pd.concat(all_dfs, ignore_index=True)

# Display the final DataFrame
display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Above this line is working

# CELL ********************

import pandas as pd

# Initialize DataFrame
df = pd.DataFrame(columns=['Province', 'Association', 'Schedule', 'Category', 'Subcategory', 'Limitations'])

# State tracking
province_set = False
association_set = False
schedule_set = False

# Row index
row = -1

# Persistent values
province = association = schedule = category = None
limitations = []

for size, text in texts_tuples:
    size = round(size, 2)

    if size == 18.0 and not province_set:
        province = text
        province_set = True

    elif size == 15.96:
        if 'Schedule' in text:
            schedule = text
            schedule_set = True
        else:
            association = text
            association_set = True

    elif size == 14.04:
        category = text

    elif size == 12.0:
        # Assign limitations to the previous row before starting a new one
        if row >= 0 and limitations:
            df.at[row, 'Limitations'] = ' '.join(limitations).strip()
            limitations = []

        # Start a new row
        row += 1
        df.at[row, 'Province'] = province
        df.at[row, 'Association'] = association
        df.at[row, 'Schedule'] = schedule
        df.at[row, 'Category'] = category
        df.at[row, 'Subcategory'] = text

    elif size == 11.04 and text.strip():
        limitations.append(text)

# Assign any remaining limitations to the last row
if row >= 0 and limitations:
    df.at[row, 'Limitations'] = ' '.join(limitations).strip()

display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

texts_tuples

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unique_first_elements = {t[0] for t in texts_tuples}
unique_first_elements


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for page in doc:
    blocks = page.get_text("dict")["blocks"]
    
    for i in range(0,len(blocks)):
        try:
            block = blocks[i]['lines']
            
            for i in range(0,len(block)-1):
                print(block[i]['spans'][0]['text'])
        except:
            continue


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in range(0,len(blocks)):
    print(i)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

block = blocks[3]['lines']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for i in range(0,len(block)-1):
    print(block[i]['spans'][0]['text'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

blocks[3]['lines']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

blocks[2]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = []  # Store structured data

for page in doc:
    blocks = page.get_text("dict")["blocks"]
    
    header, subsection = None, None
    for block in blocks:
        for line in block["lines"]:
            for span in line["spans"]:
                text = span["text"]
                
                if span["size"] > 14:  # Detect section headers
                    header = text
                    subsection = None  # Reset subsection
                
                elif span["size"] > 12:  # Detect subsections
                    subsection = text
                
                elif text.startswith(("•", "-", "›")):  # Detect bullet points
                    data.append([header, subsection, text])

# Convert extracted data into a DataFrame
df = pd.DataFrame(data, columns=["Section Header", "Subsection", "Bullet Point"])
print(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for page in doc:
    print(page.get_text("xml"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import xml.etree.ElementTree as ET

for page in doc:
    xml_text = page.get_text("xml")
    root = ET.fromstring(xml_text)

    for line in root.findall(".//line"):
        #text = line.attrib.get("text")
        #print(text)
        
        # Get the full XML string of the <line> element
        line_xml = ET.tostring(line, encoding="unicode")
        print(line_xml)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for page in doc:
    xml_text = page.get_text("xml")
    root = ET.fromstring(xml_text)

    for line in root.findall(".//line"):
        # Remove all <char> elements from the line
        for char in line.findall(".//char"):
            parent = char.getparent() if hasattr(char, 'getparent') else None
            if parent is not None:
                parent.remove(char)
            else:
                line.remove(char)

        # Convert the cleaned line back to string
        line_xml = ET.tostring(line, encoding="unicode")
        print(line_xml)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/sunlife/fee_guide_universe/2025-04-24_feeguide_universe.csv")
# df now is a Spark DataFrame containing CSV data from "Files/sunlife/fee_guide_universe/2025-04-24_feeguide_universe.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.filter(df['Procedure Code'] == '43407'))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
