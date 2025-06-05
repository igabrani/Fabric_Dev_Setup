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
# META         },
# META         {
# META           "id": "787f65b7-02b8-4b8c-930e-3405f9a89660"
# META         },
# META         {
# META           "id": "30403899-73c8-4ff0-87e6-107b5a167e84"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Load required Python packages
# Load all Python packages required for this process

# CELL ********************

import os
import pandas as pd
import pyspark.sql

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Extract data from System Table and the Data Lexicon Workbook
# This segment will load the column names and the table names from the System Table then load the Table table and Field_BRONZE, Field_SILVER, Field_GOLD, and Derived tables from the Data Lexicon Workbook

# CELL ********************

# Load the Data Lexicon (dl) Workbook data ===============================================================

dl_extract = pd.read_excel('./builtin/DataLexicon_v011.xlsx', sheet_name = [0, 1, 2, 3, 4, 6])
dl_bronze_fields =      dl_extract[0]
dl_silver_fields =      dl_extract[1]
dl_gold_fields =        dl_extract[2]
dl_calculated_fields =  dl_extract[3]
dl_master =             dl_extract[4]
dl_tables =             dl_extract[6]

# Load the System Table data =============================================================================

# Load Bronze data from files
dirs = [ 
    '/lakehouse/default/Files/CRA/copay/',
    '/lakehouse/default/Files/ESDC/Contact_Centre/',
    '/lakehouse/default/Files/ESDC/Intake/PT/Applications/',
    '/lakehouse/default/Files/ESDC/Intake/PT/Enrolled/',
    '/lakehouse/default/Files/ESDC/Members/',
    '/lakehouse/default/Files/Grouping/',
    '/lakehouse/default/Files/HC/Costing/',
    '/lakehouse/default/Files/HC/Eligible_Population/',
    '/lakehouse/default/Files/HC/Lookup_Tables/',
    '/lakehouse/default/Files/HC/Lookup_Tables/',
    '/lakehouse/default/Files/HC/Sunlife_Milestones/',
    '/lakehouse/default/Files/Mapping/',
    '/lakehouse/default/Files/Shortcut_Test/',
    '/lakehouse/default/Files/StatCan/PCCF/',
    '/lakehouse/default/Files/SunLife/CL90/Archive/',
    '/lakehouse/default/Files/SunLife/CL92/Archive/',
    '/lakehouse/default/Files/SunLife/Contact_Center/Archive/',
    '/lakehouse/default/Files/SunLife/Costing/',
    '/lakehouse/default/Files/SunLife/FI02/Archive/',
    '/lakehouse/default/Files/SunLife/Lookup_Tables/',
    '/lakehouse/default/Files/SunLife/PP08/',
    '/lakehouse/default/Files/SunLife/Provider_Billing/',
]

# This is a list of all files excluded from search (stuff that is for testing or unused)
exclude_list = [
    
]

# Load all files from each path
df = pd.DataFrame(columns = [ 'FieldName' ])

for i in dirs:
    files = os.listdir(i)

    # Screen loaded files to only include unique non-excluded files
    for j in files:

        # Filter files
        if j in exclude_list:
            continue

        # Get the file type and load the columns 
        file_type = j.split('.')
        file_type = file_type[len(file_type) - 1]

        columns = []
        if file_type == 'csv':
            columns = pd.read_csv(i + j, nrows = 0).columns
        elif file_type == 'parquet':
            columns = pd.read_parquet(i + j).columns

        

# Load silver and gold data from the lakehouse directly
#sys_silver_fields = spark.sql("SELECT * FROM Silver_Lakehouse_DEV.dbo.df;")
#print(sys_silver_fields.count())
#sys_gold_fields =   spark.sql("SELECT * FROM Gold_Lakehouse_DEV.dbo.df;")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Perform transformations & mapping
# This segment will be used to take the data that was loaded and transform it into a uniform format before it is migrated to the Data Lexicon Workbook

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
