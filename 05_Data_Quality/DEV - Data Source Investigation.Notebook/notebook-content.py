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
# META         },
# META         {
# META           "id": "911ba9c9-2599-42c5-b72c-30050958d181"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Import Classes

# CELL ********************

%run DEV - Data Source Validation Classes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run DEV - Data Source Validation Classes PySpark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # File-to-File Comparison

# MARKDOWN ********************

# ## Input

# CELL ********************

# Set parameters for file per file comparison. 

# First Data Source (src1)
src1_lvl1_loc = 'sunlife'
src1_lvl2_loc = 'sunlife_claims_universe'
src1_lakehouse = 'Silver_Lakehouse_PROD'
src1_object_type = 'tables'
src1_runType = 'single'
src1_date = '2025-04-15'

# Second Data Source (src2)
src2_lvl1_loc  = 'sunlife'
src2_lvl2_loc  = 'sunlife_cl90'
src2_lakehouse = 'Silver_Lakehouse_PROD'
src2_object_type = 'tables'
src2_runType = 'single'
src2_date = '2025-04-15'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Code Execution

# CELL ********************

source1 = DatasetImporter('src1').import_dataset()
source2 = DatasetImporter('src2').import_dataset()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source1[0].count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source2[0].count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dataframe1 = source1[0]
dataframe2 = source2[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, regexp_replace, lit, expr
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

class SparkDataFrameCleaning:
    def __init__(self, df: DataFrame):
        self.df_raw = df

    @staticmethod
    def clean_column_names(df: DataFrame) -> DataFrame:
        for col_name in df.columns:
            cleaned_name = col_name.strip().replace("  ", " ").replace("\n", " ")
            df = df.withColumnRenamed(col_name, cleaned_name)
        return df

    def clean_values(self):
        # Replace 'N/A', 'NULL', '' with None and trim spaces for string columns
        for column in self.df_raw.columns:
            self.df_raw = self.df_raw.withColumn(
                column,
                when(trim(col(column)).isin('N/A', 'NULL', ''), None)
                .otherwise(trim(col(column)))
            )

    def rename_columns(self):
        column_map_dict = {
            'Age': 'Member Age', 
            'Co-Pay(Plan ID)': 'Co-Pay (Plan ID) Category', 
            'Member Postal Code': 'Member Postal Code / Zip Code', 
            'Member Province': 'Member Province/Territory', 
            'Procedure Code': 'Procedure Code Submitted', 
            'Provider Province': 'Provider Province/Territory', 
            'Specialty': 'Provider Specialty',
            'Member Province/Territories': 'Member Province/Territory',
            'Provider Number': 'Provider ID',
            'Provider Postal Code': 'Provider Postal Code / Zip Code',
            'Provider Province/Territories': 'Provider Province/Territory',
            'CFR Date': 'Claim Funding Date',
            'Cheques': 'Cheque'
        }

        for old, new in column_map_dict.items():
            if old in self.df_raw.columns:
                self.df_raw = self.df_raw.withColumnRenamed(old, new)

    def unpivot_dataframe(self):
        # Unpivot using stack
        columns_to_unpivot = ['Payment Cancellations Cheque', 'Payment Cancellations EFT',
                              'Refunds Cheque', 'Refunds EFT', 'Voids Cheque', 'Voids EFT',
                              'Cheque', 'EFT', 'Other Adjustments']

        existing = [c for c in columns_to_unpivot if c in self.df_raw.columns]
        if not existing:
            return

        id_vars = [c for c in self.df_raw.columns if c not in existing]

        expr_str = ", ".join([f"'{col}', `{col}`" for col in existing])
        self.df_raw = self.df_raw.selectExpr(*id_vars, f"stack({len(existing)}, {expr_str}) as (Payment_Method, Paid_Amount)")
        self.df_raw = self.df_raw.filter(col("Paid_Amount") != 0)

    def pad_leading_zeros(self):
        column_pad_map = {
            'Procedure Code Paid': 5,
            'Procedure Code Submitted': 5,
            'Tooth Number': 2,
            'Provider Facility ID': 9,
            'Provider ID': 9,
            'Member Postal Code / Zip Code': 5
        }

        for column, length in column_pad_map.items():
            if column in self.df_raw.columns:
                self.df_raw = self.df_raw.withColumn(
                    column,
                    when(col(column).isNotNull(),
                         F.lpad(F.regexp_replace(col(column).cast(StringType()), r'\.0$', ''), length, '0')
                    ).otherwise(None)
                )

    def clean_mappings(self):
        # Province and specialty mappings
        province_mapping = {
            'ON': 'Ontario', 'QC': 'Quebec', 'BC': 'British Columbia',
            'AB': 'Alberta', 'MB': 'Manitoba', 'SK': 'Saskatchewan',
            'NS': 'Nova Scotia', 'NB': 'New Brunswick', 'PE': 'Prince Edward Island',
            'NL': 'Newfoundland and Labrador', 'YT': 'Yukon',
            'NT': 'Northwest Territories', 'NU': 'Nunavut'
        }

        specialty_mapping = {
            'GP': 'General practitioner', 'HY': 'Dental hygienist', 'DT': 'Denturist',
            'EN': 'Endodontist', 'OP': 'Oral pathologist', 'OR': 'Oral radiologist',
            'OS': 'Oral and maxillofacial surgeon', 'OT': 'Orthodontist', 'PD': 'Pediatric dentist',
            'PE': 'Periodontist', 'PR': 'Prosthodontist', 'OM': 'Oral medicine',
            'AN': 'Anaesthesiologist', 'DS': 'Dental school', 'DENOFF': 'Dental Office',
            'MGP': 'Medical General Practionner'
        }

        def create_mapping_expr(mapping_dict):
            return F.create_map([lit(x) for kv in mapping_dict.items() for x in kv])

        province_expr = create_mapping_expr(province_mapping)
        specialty_expr = create_mapping_expr(specialty_mapping)

        for col_name in self.df_raw.columns:
            if 'province' in col_name.lower():
                self.df_raw = self.df_raw.withColumn(
                    col_name,
                    when(province_expr.getItem(col(col_name)).isNotNull(),
                         province_expr.getItem(col(col_name)))
                    .otherwise("Unknown")
                )

            if 'specialty' in col_name.lower():
                self.df_raw = self.df_raw.withColumn(
                    col_name,
                    when(specialty_expr.getItem(col(col_name)).isNotNull(),
                         specialty_expr.getItem(col(col_name)))
                    .otherwise("Unknown")
                )

    def process(self):
        """Runs all cleaning steps in sequence."""
        self.df_raw = self.clean_column_names(self.df_raw)
        self.clean_values()
        self.rename_columns()
        self.unpivot_dataframe()
        self.pad_leading_zeros()
        self.clean_mappings()
        return self.df_raw

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dataframe1 = SparkDataFrameCleaning(dataframe1).process()
dataframe2 = SparkDataFrameCleaning(dataframe2).process()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source1 = DatasetImporter('src1').import_dataset()
source2 = DatasetImporter('src2').import_dataset()
clear_output(wait=True)

dataframe1 = source1[0]
dataframe2 = source2[0]

dataframe1 = SparkDataFrameCleaning(dataframe1).process()
clear_output(wait=True)

dataframe2 = SparkDataFrameCleaning(dataframe2).process()
clear_output(wait=True)

common_cols = sorted(set(dataframe1.columns).intersection(set(dataframe2.columns)))
df1_filtered = dataframe1[common_cols]
df2_filtered = dataframe2[common_cols]

df1_sorted = df1_filtered.sort_values(by=common_cols).reset_index(drop=True)
df2_sorted = df2_filtered.sort_values(by=common_cols).reset_index(drop=True)

df2_sorted = df2_sorted.reindex(df1_sorted.index)

result = df1_sorted.compare(df2_sorted)
result.head(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Silver_Lakehouse_PROD.sunlife.sunlife_claims_universe LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Silver_Lakehouse_PROD.sunlife.sunlife_claims_universe LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1_sorted

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2_sorted

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

comparison_result = DataFrameComparer(dataframe1, dataframe2).compare()
clear_output(wait=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

common_cols = sorted(set(dataframe1.columns).intersection(set(dataframe2.columns)))
df1_filtered = dataframe1[common_cols]
df2_filtered = dataframe2[common_cols]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1_sorted = df1_filtered.sort_values(by=common_cols).reset_index(drop=True)
df2_sorted = df2_filtered.sort_values(by=common_cols).reset_index(drop=True)

df2_sorted = df2_sorted.reindex(df1_sorted.index)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = df1_sorted.compare(df2_sorted)
result.head(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

index_number = result.index[0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1_sorted.loc[index_number,:]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2_sorted.loc[index_number,:]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result.iloc[:,2:4].drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

col = 'Claim Reference Number'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = dataframe1[col].drop_duplicates()
df2 = dataframe2[col].drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

diff_values = set(df1).symmetric_difference(set(df2))
diff_values = list(diff_values)
print(diff_values[0])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dataframe2[dataframe2['Claim Reference Number'].isin([diff_values[0]])])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dataframe1[dataframe1['Procedure Code Submitted'] == '92220'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dataframe1[dataframe1['Procedure Code Submitted'] == '96130'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reports_list = ReportGenerator(comparison_result)
metrics_result = reports_list.get_metrics_result()
metrics_result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

differences_report = reports_list.generate_report()
differences_report.keys()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    count_report = differences_report['count_report']
    display(count_report)
except:
    print("No discrepancy in count.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

set1 = set(dataframe1['Procedure Code Submitted'])
set2 = set(dataframe2['Procedure Code Submitted'])

diff = set1.symmetric_difference(set2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

full_row_dataset_1 = differences_report['df_reports']['Full Row Differences - Dataset 1']
full_row_dataset_2 = differences_report['df_reports']['Full Row Differences - Dataset 2']
row_wise_summary = differences_report['df_reports']['Row-wise Differences Summary']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

row_wise_summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the index of the first row
first_index = row_wise_summary.index[0]
print("Index of the first row:", first_index)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

row_wise_summary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get intersection of columns (as a list)
common_cols = [col for col in dataframe2.columns if col in dataframe1.columns]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('Full Row Differences - Dataset 1')
full_row_dataset_1[common_cols]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('Full Row Differences - Dataset 2')
full_row_dataset_2[common_cols]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dataframe2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

row_wise_summary.iloc[:,2:4].drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2_first_member_id = dataframe2.loc[first_index,:]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1_first_member_id = dataframe1.loc[first_index,:]['Member ID']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dataframe1[dataframe1['Member ID'] == df1_first_member_id]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dataframe2[dataframe2['Member ID'] == df1_first_member_id]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dataset Profiles

# CELL ********************

def filter_by_intersection(df1, df2, column1, column2):
    """
    Filters df1 to keep only rows where values in column1 exist in df2[column2].
    
    Parameters:
    df1 (pd.DataFrame): The DataFrame to filter.
    df2 (pd.DataFrame): The DataFrame containing values to match.
    column1 (str): The column in df1 to check.
    column2 (str): The column in df2 containing valid values.

    Returns:
    pd.DataFrame: Filtered version of df1.
    """
    intersection_values = set(df2[column2])  # Get unique values from df2[column2]
    return df1[df1[column1].isin(intersection_values)]  # Filter df1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    dataset_1_profile = filter_by_intersection(dataset_1_profile, dataset_2_profile, "Column Name", "Column Name")
    dataset_2_profile = filter_by_intersection(dataset_2_profile, dataset_1_profile, "Column Name", "Column Name")
    profile_comparison = dataset_1_profile.compare(dataset_2_profile)
    display(profile_comparison)
except:
    ("Unable to retrieve the report.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dataset_1_profile = DataProfiler(dataframe1).get_profile()
display(dataset_1_profile)

dataset_2_profile = DataProfiler(dataframe2).get_profile()
display(dataset_2_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Print Logs

# CELL ********************

display(process_debug_logs())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Overwrite the Dataframe

# CELL ********************

def read_and_filter_table(spark, table_name, universe_value, data_source_date_value):
    """
    Reads a Delta table from the Lakehouse, filters it by Universe and Data_Source_Date.

    Parameters:
        spark: SparkSession
        table_name (str): Name of the Delta table
        universe_value (str): Value to filter 'Universe' column
        data_source_date_value (str): Value to filter 'Data_Source_Date' column

    Returns:
        DataFrame: Filtered PySpark DataFrame
    """
    # Read the Delta table
    df = spark.read.format("delta").table(table_name)

    # Filter based on Universe and Data_Source_Date
    filtered_df = df.filter(
        (df["Universe"] != universe_value) &
        (df["Data_Source_Date"] != data_source_date_value)
    )

    return filtered_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
