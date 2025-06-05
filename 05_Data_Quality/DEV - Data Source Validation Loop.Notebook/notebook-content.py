# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1e63eaee-a408-4c2e-975e-e4ab7f8b08c9",
# META       "default_lakehouse_name": "Data_Quality_Lakehouse_PROD",
# META       "default_lakehouse_workspace_id": "3eb968ef-6205-4452-940d-9a6dcbf377f2",
# META       "known_lakehouses": [
# META         {
# META           "id": "1e63eaee-a408-4c2e-975e-e4ab7f8b08c9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Classes and Functions

# CELL ********************

%run DEV - Data Source Validation Classes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Loop Execution

# MARKDOWN ********************

# ### Value Options:
# 
# - *File Dates* : Applicable to multiple csv files separated by load dates. 
# - **publish** : *Yes* or *No* to publishing the result of the comparison to data quality lakehouse.
# - **write_type** : *append* or *overwrite* the tables in the data quality lakehouse.
# - **report** : *metrics, profile, comparison, all*
# - **_lvl1_loc** : If it is a csv file, then this is the *folder name* where it is in the files section. If it is a table then this is the *schema name*.
# - **_lvl2_loc** : If it is a csv file, then this could the *sub folder name* or *file name*. If it is a table then this is the *table name*.
# - **_lakehouse** : This is the name of the lakehouse where the source file/table is.
# - **_object_type** : *files* or *tables* . This refers to the source type.
# - **_runType** : *single* or *all* . Haven't tested all but it is intended for processing all the files within a folder.

# CELL ********************

# Initialize the DeltaTableWriter class
delta_writer = DeltaTableWriter(spark)

#File Dates
start_date = datetime(2025, 4, 2)
end_date = datetime(2025, 4, 12)

publish = "No"
write_type = "append"
#metrics, profile, comparison, all
report = "all" 

# First Data Source (src1)
src1_lvl1_loc = 'sunlife'
src1_lvl2_loc = 'sunlife_claims_universe'
src1_lakehouse = 'Silver_Lakehouse_PROD'
src1_object_type = 'tables'
src1_runType = 'single'

# Second Data Source (src2)
src2_lvl1_loc  = 'sunlife'
src2_lvl2_loc  = 'sunlife_cl90'
src2_lakehouse = 'Silver_Lakehouse_PROD'
src2_object_type = 'tables'
src2_runType = 'single'

# Step 1: Date Generator
dates = DateGenerator(start_date, end_date, frequency='daily').generate_dates()
#dates = date_list

# Step 2: Compare datasets per week
engine = LakehouseTableGenerator(dates)
engine.run_comparisons()

clear_output(wait=True)

# Step 3: Get final results
final_results, metrics_report, profile_report = engine.get_combined_results()

if publish == 'No':
    pass
else:
    if report == "all":
        # Write the single DataFrame to a Delta table
        delta_writer.write_to_delta_table(metrics_report, "metrics_result", write_type)

        # Write the single DataFrame to a Delta table
        delta_writer.write_to_delta_table(profile_report, "dataset_profile", write_type)

        # Loop through the dictionary of DataFrames and write each one to the Delta table
        for key, df in final_results.items():
            if not df.empty:
                delta_writer.write_to_delta_table(df, key, write_type)
        print(f"Operation for {report_type} successful.")
        
    elif report == "metrics":
        # Write the single DataFrame to a Delta table
        delta_writer.write_to_delta_table(metrics_report, "metrics_result", write_type)

        print(f"Operation for {report_type} successful.")

    elif report == "profile":
        # Write the single DataFrame to a Delta table
        delta_writer.write_to_delta_table(profile_report, "dataset_profile", write_type)
        
        print(f"Operation for {report_type} successful.")
    
    elif report == "comparisons":
        # Loop through the dictionary of DataFrames and write each one to the Delta table
        for key, df in final_results.items():
            if not df.empty:
                delta_writer.write_to_delta_table(df, key, write_type)
        print(f"Operation for {report_type} successful.")
            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

column_list = ['row_count_comparison',
 'column_count_comparison',
 'column_names_comparison',
 'missing_values_comparison',
 'unique_values_comparison',
 'duplicate_records',
 'statistical_summary_comparison',
 'categorical_distribution',
 'metrics_result',
 'dataset_profile']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#table_name = 'dataset_profile'
universe_value = 'Estimates'
data_source_date_value = '2024-07-27'

for table_name in column_list:
    remove_records_from_table(spark, table_name, universe_value, data_source_date_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#table_name = 'dataset_profile'
universe_value = 'Claims'

for data_source_date_value in date_list:
    for table_name in column_list:
        remove_records_from_table(spark, table_name, universe_value, data_source_date_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Splitting Daily Files

# MARKDOWN ********************

# I created this so that I could download a data range from OLAP then split it to daily files.

# MARKDOWN ********************

# ## Financial

# CELL ********************

import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/QA Test Files/Daily_Files/_financial_universe.csv"
df = pd.read_csv("/lakehouse/default/Files/QA Test Files/Daily_Files/_financial_universe.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df['Claim Funding Date'].drop_duplicates().head(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df['Load Date'] = df['Claim Funding Date'].str.extract(r'(\d{4}/\d{2}/\d{2})')
df['Load Date'] = pd.to_datetime(df['Load Date'], format='%Y/%m/%d', errors='coerce').dt.date
df.head(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_list = list(df['Load Date'].drop_duplicates().dropna())
date_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

date_list_clean = [datetime.strptime(str(date), '%Y-%m-%d').date() for date in date_list]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(date_list_clean)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Assume date_list is sorted and contains datetime.date objects
date_list_sorted = sorted(date_list_clean)

# Loop over pairs of (previous_date, current_date)
for i in range(1):
    curr_date = date_list_sorted[i]

    # Filter Pandas DataFrame between prev_date and curr_date (excluding prev_date)
    mask = (df['Load Date'] == curr_date)
    df_filtered_pandas = df[mask]

    # Drop the temporary column
    df_final_pandas = df_filtered_pandas.drop(columns=['Load Date']).astype(str)

    # Convert to Spark DataFrame
    df_final_spark = spark.createDataFrame(df_final_pandas)

    df_final_spark.toPandas().to_csv(f"/lakehouse/default/Files/QA Test Files/FI02 - OLAP/{curr_date}_financial_universe.csv", index=False)    
    print(f"{curr_date} : {df_final_spark.count()} records ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Assume date_list is sorted and contains datetime.date objects
date_list_sorted = sorted(date_list_clean)

# Loop over pairs of (previous_date, current_date)
for i in range(1, len(date_list_sorted)):
    prev_date = date_list_sorted[i - 1]
    curr_date = date_list_sorted[i]

    # Filter Pandas DataFrame between prev_date and curr_date (excluding prev_date)
    mask = (df['Load Date'] > prev_date) & (df['Load Date'] <= curr_date)
    df_filtered_pandas = df[mask]

    # Drop the temporary column
    df_final_pandas = df_filtered_pandas.drop(columns=['Load Date']).astype(str)

    # Convert to Spark DataFrame
    df_final_spark = spark.createDataFrame(df_final_pandas)

    df_final_spark.toPandas().to_csv(f"/lakehouse/default/Files/QA Test Files/FI02 - OLAP/{curr_date}_financial_universe.csv", index=False)    
    print(f"{curr_date} : {df_final_spark.count()} records ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Claims

# CELL ********************

import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/QA Test Files/Daily_Files/_claims_universe.csv"
df = pd.read_csv("/lakehouse/default/Files/QA Test Files/Daily_Files/_claims_universe.csv")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df['Load Date'].drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df['Load Date Parsed'] = df['Load Date'].str.extract(r'(\d{4}/\d{2}/\d{2})')
df['Load Date Parsed'] = pd.to_datetime(df['Load Date Parsed'], format='%Y/%m/%d', errors='coerce').dt.date
df.head(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

list(df['Load Date Parsed'].drop_duplicates().dropna()) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

date_list = list(df['Load Date Parsed'].drop_duplicates().dropna())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for date in date_list:
    # Filter rows where date is <= 2024-12-31
    df_filtered_pandas = df[df['Load Date Parsed'] == date]

    # Drop the temporary column
    df_final_pandas = df_filtered_pandas.drop(columns=['Load Date']).astype(str)

    df_final_spark = spark.createDataFrame(df_final_pandas)

    df_final_spark.toPandas().to_csv(f"/lakehouse/default/Files/QA Test Files/CL90 - OLAP/{date}_claims_universe.csv", index=False)    
    print(f"{date} : {df_final_spark.count()} records ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Estimates

# CELL ********************

import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/QA Test Files/Daily_Files/_estimates_universe.csv"
df = pd.read_csv("/lakehouse/default/Files/QA Test Files/Daily_Files/_estimates_universe.csv")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df['Load Date'].drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df['Load Date Parsed'] = df['Load Date'].str.extract(r'(\d{4}/\d{2}/\d{2})')
df['Load Date Parsed'] = pd.to_datetime(df['Load Date Parsed'], format='%Y/%m/%d', errors='coerce').dt.date
df.head(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

list(df['Load Date Parsed'].drop_duplicates().dropna())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

date_list = list(df['Load Date Parsed'].drop_duplicates().dropna())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for date in date_list:
    # Filter rows where date is <= 2024-12-31
    df_filtered_pandas = df[df['Load Date Parsed'] == date]

    # Drop the temporary column
    df_final_pandas = df_filtered_pandas.drop(columns=['Load Date']).astype(str)

    df_final_spark = spark.createDataFrame(df_final_pandas)

    df_final_spark.toPandas().to_csv(f"/lakehouse/default/Files/QA Test Files/CL92 - OLAP/{date}_estimates_universe.csv", index=False)    
    print(f"{date} : {df_final_spark.count()} records ")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Removing Records

# MARKDOWN ********************

# ## Removing All Result per universe

# CELL ********************

table_list = ['row_count_comparison',
 'column_count_comparison',
 'column_names_comparison',
 'missing_values_comparison',
 'unique_values_comparison',
 'duplicate_records',
 'statistical_summary_comparison',
 'categorical_distribution',
 'metrics_result',
 'dataset_profile']

for table_name in table_list:
    # Read the existing Delta table
    df = spark.read.format("delta").table(table_name)

    # Filter out rows that match BOTH conditions
    filtered_df = df.filter(
        ~((df["Universe"] == "FI02") | (df["Universe"] == "Financial"))
    )

    # Overwrite the existing Delta table with the filtered data
    (
        filtered_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_list = ['dataset_profile']

for table_name in table_list:
    # Read the existing Delta table
    df = spark.read.format("delta").table(table_name)

    # Filter out rows that match BOTH conditions
    filtered_df = df.filter(
        ~(df["Universe"] == "CL92")
    )

    # Overwrite the existing Delta table with the filtered data
    (
        filtered_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Removing Specific Date Range

# CELL ********************

table_list = ['row_count_comparison',
 'column_count_comparison',
 'column_names_comparison',
 'missing_values_comparison',
 'unique_values_comparison',
 'duplicate_records',
 'statistical_summary_comparison',
 'categorical_distribution',
 'metrics_result',
 'dataset_profile']

for table_name in table_list:
    # Read the existing Delta table
    df = spark.read.format("delta").table(table_name)

    # Filter out rows that match BOTH conditions
    filtered_df = df.filter(
        ~((df["Universe"] == "Claims") & (df["Data_Source_Date"] >= "2025-02-01") & (df["Data_Source_Date"] < "2025-04-13"))
    )

    # Overwrite the existing Delta table with the filtered data
    (
        filtered_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Done {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_list = ['row_count_comparison',
 'column_count_comparison',
 'column_names_comparison',
 'missing_values_comparison',
 'unique_values_comparison',
 'duplicate_records',
 'statistical_summary_comparison',
 'categorical_distribution',
 'metrics_result',
 'dataset_profile']

for table_name in table_list:
    # Read the existing Delta table
    df = spark.read.format("delta").table(table_name)

    # Filter out rows that match BOTH conditions
    filtered_df = df.filter(
        ~((df["Universe"] == "Financial") & (df["Data_Source_Date"].isin(date_list)))
    )
    # Overwrite the existing Delta table with the filtered data
    (
        filtered_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Done {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
