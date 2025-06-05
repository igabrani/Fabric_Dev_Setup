# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "911ba9c9-2599-42c5-b72c-30050958d181",
# META       "default_lakehouse_name": "Silver_Lakehouse_PROD",
# META       "default_lakehouse_workspace_id": "3eb968ef-6205-4452-940d-9a6dcbf377f2",
# META       "known_lakehouses": [
# META         {
# META           "id": "911ba9c9-2599-42c5-b72c-30050958d181"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Import Dataframes

# CELL ********************

lakehouse_df = spark.sql("SELECT * FROM Silver_Lakehouse_PROD.sunlife.sunlife_cl90")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

olap_df = spark.sql("SELECT * FROM Silver_Lakehouse_PROD.olap.sunlife_claims_universe")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check Dataframe Type

# CELL ********************

type(lakehouse_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

type(olap_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Re-assign Dataframes

# CELL ********************

df1 = lakehouse_df
df2 = olap_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df1.head(1))
display(df2.head(1))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Initial Check Schema Equality

# CELL ********************

if df1.schema != df2.schema:
    print("❌ DataFrames have different schemas.")
else:
    print("✅ DataFrames have the same schema.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Align Schemas

# CELL ********************

# Standardize Column Names and Order

# Get common columns (in same order) from df1
common_cols = [col for col in df1.columns if col in df2.columns]

# Reorder both DataFrames to have the same columns in the same order
df1 = df1.select(common_cols)
df2 = df2.select(common_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Align Data Types

from pyspark.sql.functions import col

for col_name, dtype in df1.dtypes:
    df2 = df2.withColumn(col_name, col(col_name).cast(dtype))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check Schema Alignment

# CELL ********************

if df1.schema != df2.schema:
    print("❌ DataFrames have different schemas.")
else:
    print("✅ DataFrames have the same schema.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df1.head(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df2.head(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Clean Data

# MARKDOWN ********************

# ### Changing Abbreviated Values

# CELL ********************

df1.select("Facility_PT", "Member_PT","Specialty").head(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# province_map = {
#     'ON': 'Ontario', 'QC': 'Quebec', 'BC': 'British Columbia',
#     'AB': 'Alberta', 'MB': 'Manitoba', 'SK': 'Saskatchewan',
#     'NS': 'Nova Scotia', 'NB': 'New Brunswick', 'PE': 'Prince Edward Island',
#     'NL': 'Newfoundland and Labrador', 'YT': 'Yukon',
#     'NT': 'Northwest Territories', 'NU': 'Nunavut'
# }
# 
# specialty_map = {
#             'GP' : 'General practitioner' ,
#             'HY' : 'Dental hygienist' ,
#             'DT' : 'Denturist' ,
#             'EN' : 'Endodontist' ,
#             'OP' : 'Oral pathologist' ,
#             'OR' : 'Oral radiologist' ,
#             'OS' : 'Oral and maxillofacial surgeon' ,
#             'OT' : 'Orthodontist' ,
#             'PD' : 'Pediatric dentist' ,
#             'PE' : 'Periodontist' ,
#             'PR' : 'Prosthodontist' ,
#             'OM' : 'Oral medicine' ,
#             'AN' : 'Anaesthesiologist' ,
#             'DS' : 'Dental school' ,
#             'DENOFF' : 'Dental Office',
#             'MGP' : 'Medical General Practionner'
#         }
# 
# df1 = df1.replace(province_map, subset=["Facility_PT", "Member_PT"])
# df1 = df1.replace(specialty_map, subset=["Specialty"])


# CELL ********************

df1.select("Facility_PT", "Member_PT","Specialty").head(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pt_unknown_map = {
            'Unknown' : '',
            'South Carolina' : 'SC',
            "Michigan": "MI",
            "Arizona": "AZ",
            "North Carolina": "NC",
            "California": "CA",
            "Pennsylvania": "PA",
            "Texas": "TX",
            "New Hampshire": "NH",
            "Florida": "FL",
            "Nevada": "NV",
            "??": ""}

df2 = df2.replace(pt_unknown_map, subset=["Member_PT"])
df1 = df1.replace(pt_unknown_map, subset=["Member_PT"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Padding a leading zero

# CELL ********************

from pyspark.sql.functions import trim, length, col, when, lpad

def pad_column_with_min_length(df, column_name, min_length):
    return df.withColumn(
        column_name,
        when(
            (length(col(column_name)) < min_length) & (trim(col(column_name)) != ""),
            lpad(col(column_name), min_length, '0')
        ).otherwise(col(column_name))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = pad_column_with_min_length(df1, "Tooth_Number", 2)
df2 = pad_column_with_min_length(df2, "Tooth_Number", 2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = pad_column_with_min_length(df1, "Facility_ID", 9)
df2 = pad_column_with_min_length(df2, "Facility_ID", 9)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = pad_column_with_min_length(df1, "Provider_ID", 9)
df2 = pad_column_with_min_length(df2, "Provider_ID", 9)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = pad_column_with_min_length(df1, "Member_Postal_Code", 5)
df2 = pad_column_with_min_length(df2, "Member_Postal_Code", 5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = pad_column_with_min_length(df1, "Member_FSA", 3)
df2 = pad_column_with_min_length(df2, "Member_FSA", 3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Replacing Null Values

# CELL ********************

from pyspark.sql.functions import trim, col, when, isnan, lower
from pyspark.sql.types import StringType

# Step 1: Identify only StringType columns
string_cols = [f.name for f in df1.schema.fields if isinstance(f.dataType, StringType)]

for c in string_cols:
    df1 = df1.withColumn(
        c,
        when(
            (trim(col(c)) == "") |
            (lower(col(c)) == "null") |
            (col(c) == "nan") |
            isnan(col(c)),
            None
        ).otherwise(col(c))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

string_cols = [f.name for f in df2.schema.fields if isinstance(f.dataType, StringType)]

for c in string_cols:
    df2 = df2.withColumn(
        c,
        when(
            (trim(col(c)) == "") |
            (lower(col(c)) == "null") |
            (col(c) == "nan") |
            isnan(col(c)),
            None
        ).otherwise(col(c))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

string_cols

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Re-assigning Dataframe

# CELL ********************

revised_df1 = df1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns = revised_df1.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

drop_columns = ['SL_Procedure_Submitted_Description_EN',
                'SL_Procedure_Submitted_Description_FR',
                'SL_Procedure_Paid_Description_EN',
                'SL_Procedure_Paid_Description_FR', 
                'Reason_Desription',
                'Remark_Description',
                #Inconsistent flagging
                #'QC_Flag',
                #'Code_Join_Submitted',
                #'Code_Join_Paid',
                #Inconsistent Benefit Category
                'Benefit_Category']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

important_columns = [col for col in columns if col not in drop_columns]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PySpark Code to Compare Two Large DataFrames

# CELL ********************

dates = [
        '2025-05-21',
        '2025-05-17',
        '2025-05-16',
        '2025-05-15',
        '2025-05-14',
        '2025-05-13',
        '2025-05-09',
        '2025-05-08',
        '2025-05-07',
        '2025-05-06',
        '2025-05-02',
        '2025-05-01'
        ]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sha2, concat_ws, col, broadcast, md5
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

'''
def get_row_hash_df(df, cols):
    # Select all important columns plus the hash column for joining back
    return df.select(
        sha2(concat_ws("||", *cols), 256).alias("row_hash"),
        *cols
    )
'''
def get_row_hash_df(df, cols, keep_column):
    return df.select(
        md5(concat_ws("||", *cols)).alias("row_hash"),
        col(keep_column)
    )

# Get unique source dates
df1_dates = [str(row['source']) for row in revised_df1.select('source').distinct().collect()]
df2_dates = [str(row['source']) for row in df2.select('source').distinct().collect()]
common_dates = list(set(df1_dates).intersection(set(df2_dates)))

important_attribute = "claim_reference_number"

# Log list for storing results and samples
comparison_log = []

# Number of sample rows to extract for unmatched records
sample_size = 2

for load_date in sorted(dates):
    df1_filtered = revised_df1.filter(col('source') == load_date) #((col('source') == load_date) & (col('Member_PT') == 'ON'))
    #print(f'df1 {load_date} - {df1_filtered.count()}')
    df2_filtered = df2.filter(col('source') == load_date) #((col('source') == load_date) & (col('Member_PT') == 'ON'))
    #print(f'df2 {load_date} - {df2_filtered.count()}')

    df1_hashed = get_row_hash_df(df1_filtered, important_columns, important_attribute).cache()
    df2_hashed = get_row_hash_df(df2_filtered, important_columns, important_attribute).cache()
    #print("Done hashing")

    # Find unmatched hashes
    diff1 = df1_hashed.select("row_hash").subtract(df2_hashed.select("row_hash")).cache()
    diff2 = df2_hashed.select("row_hash").subtract(df1_hashed.select("row_hash")).cache()
    #print("Done getting the difference")

    diff1_nonempty = bool(diff1.take(1))
    diff2_nonempty = bool(diff2.take(1))
    identical = not (diff1_nonempty or diff2_nonempty)
    #print("Done comparing")

    # Limit the diff datasets to top N hashes
    diff1_sample = diff1.limit(sample_size)
    diff2_sample = diff2.limit(sample_size)

    #'''
    # Join back unmatched hashes to get full row samples
    unmatched_in_df1 = df1_hashed.join(broadcast(diff1_sample), "row_hash", "inner").drop("row_hash")
    unmatched_in_df2 = df2_hashed.join(broadcast(diff2_sample), "row_hash", "inner").drop("row_hash")
    #print("Done fetching unmatched information")

    # Collect claim reference sample values as a flat list
    # Collect and concatenate sample value
    sample_diff1 = (
        ", ".join([str(row[important_attribute]) for row in unmatched_in_df1.select(important_attribute).limit(sample_size).collect()])
        if diff1_nonempty else ""
    )

    sample_diff2 = (
        ",".join([str(row[important_attribute]) for row in unmatched_in_df2.select(important_attribute).limit(sample_size).collect()])
        if diff2_nonempty else ""
    )

    #print("Done fetching unmatched claim reference number")
    print(f'Done processing {load_date}')

    '''
    sample_diff1 = ''
    sample_diff2 = ''
    '''

    comparison_log.append({
        'source': str(load_date),
        'identical': 'Yes' if identical else 'No',
        'diff1_count': int(diff1.count()),
        'diff2_count': int(diff2.count()),
        'missing_in_df2': 'Yes' if diff1_nonempty else 'None',
        'missing_in_df1': 'Yes' if diff2_nonempty else 'None',
        'sample_claim_ref_in_df1_not_in_df2': sample_diff1,
        'sample_claim_ref_in_df2_not_in_df1': sample_diff2
    })

# Schema for summary
schema = StructType([
    StructField('source', StringType(), True),
    StructField('identical', StringType(), True),
    StructField('diff1_count', IntegerType(), True),
    StructField('diff2_count', IntegerType(), True),
    StructField('missing_in_df2', StringType(), True),
    StructField('missing_in_df1', StringType(), True),
    StructField('sample_claim_ref_in_df1_not_in_df2', StringType(), True),
    StructField('sample_claim_ref_in_df2_not_in_df1', StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log_df = spark.createDataFrame(comparison_log, schema=schema)
display(log_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

comparison_log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sampling

# CELL ********************

reference_sample = "040425-AHU19-00"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample1 = revised_df1.where(revised_df1["Claim_Reference_Number"] == reference_sample)
sample2 = df2.where(df2["Claim_Reference_Number"] == reference_sample)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sample1.select(important_columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sample2.select(important_columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
