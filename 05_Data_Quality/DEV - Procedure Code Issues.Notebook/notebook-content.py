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

# # Data Sources

# MARKDOWN ********************

# ## Fee Guide Universe

# CELL ********************

df_fee_guide = spark.read.format("csv").option("header","true").load("Files/sunlife/fee_guide_universe/2025-04-24_feeguide_universe.csv")
# df now is a Spark DataFrame containing CSV data from "Files/sunlife/fee_guide_universe/2025-04-24_feeguide_universe.csv".
display(df_fee_guide)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fact Claims (CL90)

# CELL ********************

columns = ['Claim_Reference_Number',
        'Member_ID',
        'Member_PT',
        'Facility_PT',
        'Specialty',
        'Procedure_Code_Submitted',
        'Procedure_Code_Paid',
        'Tooth_Number',
        'Tooth_Surface',
        'Service_Date',
        'Claim_Date',
        'Submitted_Date',
        'Adjudicated_Date',
        'Amended_Date',
        'Analysis_Date',
        'Reason_Code',
        'Remark_Code',
        'Submitted_Amount',
        'Eligible_Amount',
        'COB_Amount',
        'Paid_Amount',
        'Source']

query = f"SELECT {', '.join(columns)} FROM CDCP.Gold_Lakehouse_PROD.sunlife_gold.fact_claims"

df_fact_claims = spark.sql(query)
display(df_fact_claims)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## HC Procedure Codes (2024 ABCD Document)

# CELL ********************

df_abcd_codes = spark.sql("SELECT * FROM CDCP.Gold_Lakehouse_PROD.hc.dim_procedure_codes")
display(df_abcd_codes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Active Dummy Codes from SunLife

# CELL ********************

df_dummy_codes = spark.read.format("csv").option("header","true").load("Files/sunlife/active_dummy_codes/Active Dummy Procedure Codes 12052025.csv")
# df now is a Spark DataFrame containing CSV data from "Files/sunlife/active_dummy_codes/Active Dummy Procedure Codes 12052025.csv".

df_dummy_codes = df_dummy_codes.select(df_dummy_codes.columns[0:9])

# Function to convert column names to Title Case with underscores
def format_column_name(col_name):
    return col_name.title().replace(" ", "_")

# Apply the renaming to all columns
df_dummy_codes = df_dummy_codes.toDF(*[format_column_name(col) for col in df_dummy_codes.columns])

display(df_dummy_codes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Analysis

# MARKDOWN ********************

# **Table Names**
# - df_fee_guide
# - df_fact_claims
# - df_abcd_codes
# - df_dummy_codes

# CELL ********************

display(df_fact_claims)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame

def left_join_custom_keys(
    df_left: DataFrame,
    df_right: DataFrame,
    left_keys: list,
    right_keys: list,
    right_cols_to_keep: list
) -> DataFrame:
    """
    Performs a left join between df_left and df_right using different keys,
    retains all columns from df_left, and only selected columns from df_right
    (excluding the right join keys).

    Parameters:
    - df_left: Left DataFrame
    - df_right: Right DataFrame
    - left_keys: List of column names from df_left to join on
    - right_keys: List of column names from df_right to join on
    - right_cols_to_keep: List of column names from df_right to include in the result

    Returns:
    - A new DataFrame with the join applied
    """
    # Create join condition
    join_condition = [df_left[l] == df_right[r] for l, r in zip(left_keys, right_keys)]

    # Select only the desired columns from the right DataFrame (excluding join keys)
    right_cols_final = [col for col in right_cols_to_keep if col not in right_keys]
    right_selected = df_right.select(right_keys + right_cols_final)

    # Perform the join
    joined_df = df_left.join(right_selected, on=join_condition, how="left")

    # Drop the right-side join keys
    for rk in right_keys:
        if rk in joined_df.columns:
            joined_df = joined_df.drop(df_right[rk])

    return joined_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main_df = left_join_custom_keys(
    df_left=df_fact_claims,
    df_right=df_abcd_codes,
    left_keys=["Procedure_Code_Submitted"],
    right_keys=["Code_Base"],
    right_cols_to_keep=["Code","Association"]
)

main_df = main_df.withColumnRenamed("Code", "SC_ABCD")
main_df = main_df.withColumnRenamed("Association", "SC_Association")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(main_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main_df = left_join_custom_keys(
    df_left=main_df,
    df_right=df_abcd_codes,
    left_keys=["Procedure_Code_Paid"],
    right_keys=["Code_Base"],
    right_cols_to_keep=["Code","Association"]
)

main_df = main_df.withColumnRenamed("Code", "PC_ABCD")
main_df = main_df.withColumnRenamed("Association", "PC_Association")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(main_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when, col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main_df = main_df.withColumn(
    "Submitted_Code_Tag",
    when(col("SC_Association").isNotNull(), "In ABCD")
    .when(col("SC_Association").isNull(), "Not in ABCD")
    .otherwise("Unknown")
)

main_df = main_df.withColumn(
    "Paid_Code_Tag",
    when(col("PC_Association").isNotNull(), "In ABCD")
    .when(col("PC_Association").isNull(), "Not in ABCD")
    .otherwise("Unknown")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(main_df.head(100))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main_df = left_join_custom_keys(
    df_left=main_df,
    df_right=df_dummy_codes,
    left_keys=["Procedure_Code_Submitted"],
    right_keys=["Dummy_Code"],
    right_cols_to_keep=["Procedure_Code"]
)

main_df = main_df.withColumnRenamed("Procedure_Code", "Dummy_SC_Actual_Code")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

main_df = left_join_custom_keys(
    df_left=main_df,
    df_right=df_dummy_codes,
    left_keys=["Procedure_Code_Paid"],
    right_keys=["Dummy_Code"],
    right_cols_to_keep=["Procedure_Code"]
)

main_df = main_df.withColumnRenamed("Procedure_Code", "Dummy_PC_Actual_Code")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(main_df.head(100))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when

main_df = main_df.withColumn(
    "SC_Final_Code",
    when(col("Submitted_Code_Tag") == "In ABCD", col("Procedure_Code_Submitted"))
    .when(
        (col("Submitted_Code_Tag") == "Not in ABCD") & (col("Dummy_SC_Actual_Code").isNotNull()),
        col("Dummy_SC_Actual_Code")
    )
    .otherwise("Unknown")
)

main_df = main_df.withColumn(
    "PC_Final_Code",
    when(col("Paid_Code_Tag") == "In ABCD", col("Procedure_Code_Paid"))
    .when(
        (col("Paid_Code_Tag") == "Not in ABCD") & (col("Dummy_PC_Actual_Code").isNotNull()),
        col("Dummy_PC_Actual_Code")
    )
    .otherwise("Unknown")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(main_df.head(100))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

filtered_df = main_df.filter(
    (col("SC_Final_Code") == "Unknown") | (col("PC_Final_Code") == "Unknown")
)

missing_sc_df = main_df.filter(col("SC_Final_Code") == "Unknown")

missing_pc_df = main_df.filter(col("PC_Final_Code") == "Unknown")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f'Either Submitted or Paid are Unknown: {filtered_df.count()}')
print(f'Submitted Codes are Unknown: {missing_sc_df.count()}')
print(f'Paid Codes are Unknown: {missing_pc_df.count()}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

selected_columns = ['Claim_Reference_Number',
                    'Member_ID',
                    'Member_PT',
                    'Facility_PT',
                    'Specialty',
                    'SC_Final_Code',
                    'PC_Final_Code',
                    'Procedure_Code_Submitted',
                    'Procedure_Code_Paid',
                    'Tooth_Number',
                    'Tooth_Surface',
                    'Service_Date',
                    'Claim_Date',
                    'Submitted_Date',
                    'Adjudicated_Date',
                    'Amended_Date',
                    'Analysis_Date',
                    'Reason_Code',
                    'Remark_Code',
                    'Submitted_Amount',
                    'Eligible_Amount',
                    'Paid_Amount',
                    'Source']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(missing_sc_df.select(selected_columns).filter(col('Paid_Amount') != 0))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

missing_sc_df = missing_sc_df.select(selected_columns).filter(col('Paid_Amount') != 0)
missing_pc_df = missing_pc_df.select(selected_columns).filter(col('Paid_Amount') != 0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

procedure_code_submitted_list = (
    missing_sc_df
    .select("Procedure_Code_Submitted")
    .dropDuplicates()
    .rdd
    .flatMap(lambda x: x)
    .collect()
)

len(procedure_code_submitted_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(procedure_code_submitted_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

procedure_code_paid_list = (
    missing_pc_df
    .select("Procedure_Code_Paid")
    .dropDuplicates()
    .rdd
    .flatMap(lambda x: x)
    .collect()
)

len(procedure_code_paid_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, count, sum

# Group by 'Procedure_Code_Submitted' and aggregate
summary_df = missing_sc_df.groupBy("Procedure_Code_Submitted","Procedure_Code_Paid","Remark_Code","Reason_Code").agg(
    count("*").alias("Record_Count"),
    sum("Paid_Amount").alias("Total_Paid_Amount")
)

# Show the result
display(summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Group by 'Procedure_Code_Submitted' and aggregate
summary_df = missing_pc_df.groupBy("Procedure_Code_Submitted","Procedure_Code_Paid","Remark_Code","Reason_Code").agg(
    count("*").alias("Record_Count"),
    sum("Paid_Amount").alias("Total_Paid_Amount")
)

# Show the result
display(summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

lh_file_path ='abfss://4748fbe5-9b18-4aac-9d74-f79c39ff81db@onelake.dfs.fabric.microsoft.com/b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe/Files/hc/price_files/'
all_dataframes = []

for fiscal_year, filename_template in fiscal_years.items():
    file_path = f'{lh_file_path}{fiscal_year} Price Files'
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

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
