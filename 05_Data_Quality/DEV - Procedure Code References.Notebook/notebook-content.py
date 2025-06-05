# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "787f65b7-02b8-4b8c-930e-3405f9a89660",
# META       "default_lakehouse_name": "Silver_Lakehouse_DEV",
# META       "default_lakehouse_workspace_id": "4748fbe5-9b18-4aac-9d74-f79c39ff81db",
# META       "known_lakehouses": [
# META         {
# META           "id": "b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe"
# META         },
# META         {
# META           "id": "aee93c31-04bd-4a97-9465-a0464f0a841a"
# META         },
# META         {
# META           "id": "787f65b7-02b8-4b8c-930e-3405f9a89660"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Price Files

# CELL ********************

from pyspark.sql.functions import col, count, sum

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from datetime import datetime

class CDCPPriceFileLoader:
    def __init__(self, fiscal_year: int, file_path: str, filename_template: str):
        self.fiscal_year = fiscal_year
        self.file_path = file_path
        self.filename_template = filename_template
        self.province_names = {
            'AB': 'Alberta',
            'BC': 'British Columbia',
            'MB': 'Manitoba',
            'NB': 'New Brunswick',
            'NL': 'Newfoundland and Labrador',
            'NS': 'Nova Scotia',
            'NT': 'Northwest Territories',
            'NU': 'Nunavut',
            'ON': 'Ontario',
            'PE': 'Prince Edward Island',
            'QC': 'Quebec',
            'SK': 'Saskatchewan',
            'YK': 'Yukon'
        }
        self.valid_from = pd.to_datetime(f"{fiscal_year}-04-01")
        self.valid_to = pd.to_datetime(f"{fiscal_year + 1}-03-31")

    def load_files(self):
        dataframes = []

        for abbr, full_name in self.province_names.items():
            filename = self.filename_template.format(year=self.fiscal_year, province=abbr)
            file_path = f"{self.file_path}/{filename}"

            try:
                # Read all sheets from the Excel file
                all_sheets = pd.read_excel(file_path, sheet_name=None, engine='openpyxl')

                for sheet_name, df in all_sheets.items():
                    # Standardize column names
                    df.columns = [col.strip().title().replace(" ", "_").replace("New_", "") for col in df.columns]

                    # Add metadata
                    df['Province_Abbr'] = abbr
                    df['Province'] = full_name
                    df['Valid_From'] = self.valid_from
                    df['Valid_To'] = self.valid_to
                    df['Provider_Type'] = sheet_name

                    # Pad Procedure_Code if it exists
                    if 'Procedure_Code' in df.columns:
                        df['Procedure_Code'] = df['Procedure_Code'].astype(str).str.zfill(5)

                    dataframes.append(df)

            except FileNotFoundError:
                print(f"File not found for province: {abbr} — skipping.")
            except Exception as e:
                print(f"Error reading file for province {abbr}: {e}")

        if dataframes:
            return pd.concat(dataframes, ignore_index=True, sort=False)
        else:
            print("No files were loaded.")
            return pd.DataFrame()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fiscal_year = 2025
lh_file_path ='abfss://4748fbe5-9b18-4aac-9d74-f79c39ff81db@onelake.dfs.fabric.microsoft.com/b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe/Files/hc/price_files/'
file_path = f'{lh_file_path}{fiscal_year} Price Files'
filename_template = f"{fiscal_year}_{{province}}_CDCP_PRICE_FILE.xlsx"

loader = CDCPPriceFileLoader(
    fiscal_year=fiscal_year,
    file_path=file_path,
    filename_template=filename_template
)

combined_df = loader.load_files()
display(combined_df.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fiscal_years = {
    2024: "{fiscal_year}_{province}_CDCP_SCHED AB_PRICE_FILE.xlsx",
    2025: "{fiscal_year}_{province}_CDCP_PRICE_FILE.xlsx"
}

lh_file_path ='abfss://4748fbe5-9b18-4aac-9d74-f79c39ff81db@onelake.dfs.fabric.microsoft.com/b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe/Files/hc/price_files'
all_dataframes = []

for fiscal_year, filename_template in fiscal_years.items():
    file_path = f'{lh_file_path}/{fiscal_year} Price Files'
    formatted_template = filename_template.format(fiscal_year=fiscal_year, province="{province}")
    
    loader = CDCPPriceFileLoader(
        fiscal_year=fiscal_year,
        file_path=file_path,
        filename_template=formatted_template
    )
    
    df = loader.load_files()
    all_dataframes.append(df)

# Combine all dataframes into one
combined_df = pd.concat(all_dataframes, ignore_index=True)
display(combined_df.head())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

combined_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert pandas DataFrame to Spark
df_spark = spark.createDataFrame(combined_df)

# Save the table
df_spark.write.mode("overwrite").saveAsTable("hc.hc_price_schedule")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Silver_Lakehouse_DEV.hc.hc_price_schedule")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ABCD Document

# CELL ********************

import pandas as pd

def import_excel_sheet(file_path: str, filename: str, sheet_name: str) -> pd.DataFrame:
    """
    Imports a specific sheet from an Excel file in a Fabric notebook.

    Parameters:
    - file_path (str): The directory path where the Excel file is stored.
    - filename (str): The name of the Excel file (e.g., 'data.xlsx').
    - sheet_name (str): The name of the sheet to import.

    Returns:
    - pd.DataFrame: The loaded data as a pandas DataFrame.
    """
    full_path = f"{file_path}/{filename}"
    try:
        df = pd.read_excel(full_path, sheet_name=sheet_name, engine='openpyxl')
        print(f"Successfully loaded sheet '{sheet_name}' from '{filename}'")
        return df
    except FileNotFoundError:
        print(f"File not found: {full_path}")
    except Exception as e:
        print(f"Error loading Excel sheet: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

file_path="abfss://4748fbe5-9b18-4aac-9d74-f79c39ff81db@onelake.dfs.fabric.microsoft.com/b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe/Files/hc/abcd_document"
filename="CDCP ABCD 2025.xlsx"
sheet_name="GPSP"

df = import_excel_sheet(
    file_path,
    filename,
    sheet_name
)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_dataframe(df):
    # Delete the first row
    df = df.drop(index=0)
    
    # Use the second row as the new header
    df.columns = df.iloc[0]
    df = df.drop(index=1)
    
    # Replace 'ü' with 'YES'
    df = df.replace('ü', 'YES')
    
    # Replace all null values with empty strings
    df = df.fillna('')
    
    # Drop the last column
    df = df.iloc[:, :-1]
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned_df = clean_dataframe(df)
display(cleaned_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned_df.columns = [
    '#', 'Description', 'CDA_Codes',
    'ACDQ_FDSQ_Code',
    'Définition', 'A', 'B', 'Z', 'C',
    'D', 'Notes'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(cleaned_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cleaned_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def fill_empty_cells(df, columns):
    """
    Fill empty cells in specified columns by copying the value from the row above.

    Parameters:
    - df (pd.DataFrame): The DataFrame to process.
    - columns (list): List of column names to fill empty cells.

    Returns:
    - pd.DataFrame: DataFrame with filled empty cells.
    """
    for column in columns:
        # Convert empty strings to None so they can be forward-filled
        df[column] = df[column].apply(lambda x: None if x == '' else x)
        df[column] = df[column].ffill()
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example usage
columns_to_fill = ['Description', 'Définition']
cleaned_df = fill_empty_cells(cleaned_df, columns_to_fill)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(cleaned_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import re

def classify_descriptions(df):
    # Ensure required columns exist
    required_columns = ['Description', 'A', 'B', 'C', 'Z', 'D']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Initialize new columns
    df['Category'] = ''
    df['Class'] = ''
    df['Subclass'] = ''
    df['Procedure_Description'] = ''

    yes_columns = ['A', 'B', 'C', 'Z', 'D']

    for i in range(len(df)):
        description = df.at[i, 'Description']

        # Fix regex: use raw string and single backslash
        if isinstance(description, str) and re.match(r'\d{5} Series: .* Services', description):
            df.at[i, 'Category'] = description

        if i + 1 < len(df) and any(df.at[i + 1, col] == 'YES' for col in yes_columns):
            df.at[i + 1, 'Class'] = df.at[i + 1, 'Description']

        if i + 2 < len(df) and any(df.at[i + 2, col] == 'YES' for col in yes_columns):
            df.at[i + 2, 'Subclass'] = df.at[i + 2, 'Description']

        if i + 3 < len(df) and any(df.at[i + 3, col] not in ['', None] for col in yes_columns):
            df.at[i + 3, 'Procedure_Description'] = df.at[i + 3, 'Description']

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_df = cleaned_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

required_columns = ['Description', 'A', 'B', 'C', 'Z', 'D']
for col in required_columns:
    if col not in sample_df.columns:
        raise ValueError(f"Missing required column: {col}")

# Initialize new columns
sample_df['Category'] = ''
sample_df['Class'] = ''
sample_df['Subclass'] = ''
sample_df['Procedure_Description'] = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

yes_columns = ['A', 'B', 'C', 'Z', 'D']

for i in range(len(sample_df)):
    description = sample_df.at[i, 'Description']

    # Fix regex: use raw string and single backslash
    if isinstance(description, str) and re.match(r'\d{5} Series: .* Services', description):
        sample_df.at[i, 'Category'] = description

    if i + 1 < len(sample_df) and any(sample_df.at[i + 1, col] == 'YES' for col in yes_columns):
        sample_df.at[i + 1, 'Class'] = sample_df.at[i + 1, 'Description']

    if i + 2 < len(sample_df) and any(sample_df.at[i + 2, col] == 'YES' for col in yes_columns):
        sample_df.at[i + 2, 'Subclass'] = sample_df.at[i + 2, 'Description']

    if i + 3 < len(sample_df) and any(sample_df.at[i + 3, col] not in ['', None] for col in yes_columns):
        sample_df.at[i + 3, 'Procedure_Description'] = sample_df.at[i + 3, 'Description']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_df.count()

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
