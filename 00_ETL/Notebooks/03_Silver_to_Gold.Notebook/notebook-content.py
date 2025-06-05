# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "30403899-73c8-4ff0-87e6-107b5a167e84",
# META       "default_lakehouse_name": "Gold_Lakehouse_DEV",
# META       "default_lakehouse_workspace_id": "4748fbe5-9b18-4aac-9d74-f79c39ff81db",
# META       "known_lakehouses": [
# META         {
# META           "id": "30403899-73c8-4ff0-87e6-107b5a167e84"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run 00_Load_Packages

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mount bronze lakehouse
workspace = spark.conf.get("trident.workspace.id")

if workspace == "4748fbe5-9b18-4aac-9d74-f79c39ff81db": #dev
    notebookutils.fs.mount("abfss://4748fbe5-9b18-4aac-9d74-f79c39ff81db@onelake.dfs.fabric.microsoft.com/b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe", "/lakehouse/Bronze_Lakehouse")
    bronze_lakehouse_id = "b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe"
    silver_lakehouse_name = "Silver_Lakehouse_DEV"
    gold_lakehouse_name = "Gold_Lakehouse_DEV"
elif workspace == "ec6eb7c4-c224-4908-8c57-cd348b9f58f4": #test
    notebookutils.fs.mount("abfss://ec6eb7c4-c224-4908-8c57-cd348b9f58f4@onelake.dfs.fabric.microsoft.com/c2b72635-1f18-4f8b-909a-94726216cc87", "/lakehouse/Bronze_Lakehouse")
    bronze_lakehouse_id = "c2b72635-1f18-4f8b-909a-94726216cc87"
    silver_lakehouse_name = "Silver_Lakehouse_TEST"
    gold_lakehouse_name = "Gold_Lakehouse_TEST"
elif workspace == "3eb968ef-6205-4452-940d-9a6dcbf377f2": #prod
    notebookutils.fs.mount("abfss://3eb968ef-6205-4452-940d-9a6dcbf377f2@onelake.dfs.fabric.microsoft.com/dee2baf0-f8c0-4682-9154-bf0a213e41ee", "/lakehouse/Bronze_Lakehouse")
    bronze_lakehouse_id = "dee2baf0-f8c0-4682-9154-bf0a213e41ee"
    silver_lakehouse_name = "Silver_Lakehouse_PROD"
    gold_lakehouse_name = "Gold_Lakehouse_PROD"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_co_pay_tier = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.cra.cra_copay_tiers')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Members

# CELL ********************

df_members = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.esdc.dim_members')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Create unique Surrogate Key to join to other datasets

# CELL ********************

# Create 'non_null_count to help break ties in rank
df_members = df_members.withColumn(
    "non_null_count",
    sum(
        when(
            (col(c).isNotNull() & (col(c) != lit(""))) | (to_date(col(c)).isNotNull()),
            1
        ).otherwise(0)
        for c in df_members.columns
    )
)

# Window Spec to create 'rank_1' and 'unique_key_temp1'
window_spec_client_number = Window.partitionBy('esdc_clientnumber').orderBy('effective_date', 'esdc_dateofapplication', col('non_null_count').desc(), 'end_date')

# Add 'rank_temp' column
df_members = df_members.withColumn('rank_temp', F.rank().over(window_spec_client_number))

# Add 'unique_key_temp' column
df_members = df_members.withColumn('unique_key_temp', F.concat(df_members['esdc_clientnumber'], F.lit("-"), df_members['rank_temp']))

# Window Spec on 'unique_key_temp'
window_spec_unique_key_temp = Window.partitionBy('unique_key_temp').orderBy('unique_key_temp')

# Create 'group_size' column to identify duplicates
df_members = df_members.withColumn('group_size', F.count('unique_key_temp').over(window_spec_unique_key_temp))

# Create 'next_uniqueid_with_size_1' column to identify 'uniqueid' of the next row after the duplicate group
df_members = df_members.withColumn(
    'next_uniqueid_with_size_1',
    F.first(when(col('group_size') == 1, col('uniqueid')), ignorenulls=True).over(window_spec_client_number.rowsBetween(1, Window.unboundedFollowing))
)

# Create 'rank' column
# If 'unique_key_temp' is a duplicate and 'uniqueid' == 'next_uniqueid_with_size_1', add 1 to 'rank_temp' to make that the leading row
df_members = df_members.withColumn(
    'rank',
    when(
        (col('group_size') > 1) & (col('uniqueid') == col('next_uniqueid_with_size_1')),
        col('rank_temp') + 1
    ).otherwise(col('rank_temp'))
)

# Re-create unique_key
df_members = df_members.withColumn('unique_key', F.concat(df_members['esdc_clientnumber'], F.lit("-"), df_members['rank']))

# Window Spec on 'unique_key'
window_spec_unique_key = Window.partitionBy('unique_key').orderBy('unique_key')

# Add a row number to identify duplicates
df_members = df_members.withColumn('row_number', F.row_number().over(window_spec_unique_key))

# Modify 'unique_key' to append suffix for duplicates
df_members = df_members.withColumn(
    'unique_key',
    when(col('row_number') == 1, col('unique_key'))  # Keep the original value for the first occurrence
    .otherwise(concat_ws('-', col('unique_key'), col('row_number').cast("string")))  # Append suffix for duplicates
)

df_members = df_members.drop('rank_temp', 'unique_key_temp', 'group_size', 'next_uniqueid_with_size_1', 'row_number', 'rank', 'non_null_count')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_members = df_members.join(df_co_pay_tier, df_members['esdc_socialinsurancenumber'] == df_co_pay_tier['SIN'], how = "left").select(df_members["*"], df_co_pay_tier["CRA_Co_Pay_Tier"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = Window.partitionBy('esdc_clientnumber')

df_members = df_members.withColumn(
    'current_record_client',
    F.when(
        F.col('unique_key') == F.max('unique_key').over(window_spec), 1
    ).otherwise(0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = Window.partitionBy('uniqueid')

df_members = df_members.withColumn(
    'current_record_application',
    F.when(
        F.col('unique_key') == F.max('unique_key').over(window_spec), 1
    ).otherwise(0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Temp until no measures have "current_record"
window_spec = Window.partitionBy('uniqueid')

df_members = df_members.withColumn(
    'current_record',
    F.when(
        F.col('effective_date') == F.max('effective_date').over(window_spec), 1
    ).otherwise(0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_members = df_members.withColumn(
    "actively_covered",
    F.when(
        (F.col('enrolled_flag') == 1)
        & (((F.current_date() >= F.col("esdc_coveragestart"))
        & (F.current_date() <= F.col("esdc_coverageend")))
        | (F.col('esdc_coverageend').isNull())),
        1
    ).otherwise(0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_members = df_members.withColumn(
    'age_current',
    F.floor(F.datediff(F.current_date(), F.col("esdc_dateofbirth")) / 365.25)
)

df_members = df_members.withColumn(
    'age_as_of_application',
    F.floor(F.datediff(F.col('esdc_dateofapplication'), F.col("esdc_dateofbirth")) / 365.25)
)

df_members = df_members.withColumn(
    'age_as_of_application',
    F.when(
        ((F.col('age_as_of_application').isNull())
        | (F.col('age_as_of_application') < 0)),
        F.floor(F.datediff(F.col('esdc_eligibleonfirst'), F.col("esdc_dateofbirth")) / 365.25)
    ).otherwise(F.col('age_as_of_application'))
)

df_members = df_members.withColumn(
    'age_as_of_application',
    F.when(
        (F.col('age_as_of_application') < 0),
        F.lit(0)
    ).otherwise(F.col('age_as_of_application'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_members.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/esdc_gold/dim_members")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # FI02

# MARKDOWN ********************

# Once current_record is fixed, add to FI02 as member_validity variable

# CELL ********************

df_fi02 = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.sunlife.sunlife_fi02')
df_providers = spark.sql(f'SELECT * FROM {gold_lakehouse_name}.sunlife_gold.dim_providers')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fi02 = df_fi02.withColumn("Provider_Facility_UID", concat_ws("-", col("Provider_ID"), col("Facility_ID")))
df_fi02 = df_fi02.join(df_providers, on = "Provider_Facility_UID", how = "left").select(df_fi02["*"], df_providers["Provider_Validity"], df_providers["Enrolled_Status"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = Window.partitionBy('Member_ID').orderBy(F.desc('Claim_Date'))

# Get the value of B where C is max in each group of D
df_fi02 = df_fi02.withColumn('Member_PT_Most_Recent', F.first('Member_PT').over(window_spec))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fi02 = df_fi02.join(df_co_pay_tier, on = "Member_ID", how = "left").select(df_fi02["*"], df_co_pay_tier["CRA_Co_Pay_Tier"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fi02.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/sunlife_gold/fact_claims_financial")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CL90

# CELL ********************

df_cl90 = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.sunlife.sunlife_cl90')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### TEST - join to members

# CELL ********************

#df_members = spark.sql(f'SELECT * FROM {gold_lakehouse_name}.esdc_gold.dim_members')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(df_members.filter(col('esdc_familyid') == "62132521179"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(df_cl90.count())
#display(df_cl90.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#test = df_cl90.join(df_members.withColumnRenamed('source', 'source_members'), df_cl90.Member_ID == df_members.esdc_clientnumber, how = "left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''window_spec = Window.partitionBy('Claim_Reference_Number')

test = test.withColumn(
    'filter',
    when(
        (col('Service_Date') >= "2024-11-14")
        & (col('end_date') >= col('Service_Date'))
        & (col('effective_date') <= col('Service_Date'))
        , 1)
    .otherwise(0)
)

test = test.withColumn(
    'filter',
    when(
        (col('Service_Date') < "2024-11-14")
        & (col('unique_key') == F.min('unique_key').over(window_spec))
        , 1
    ).otherwise(col('filter'))
)

test = test.withColumn(
    'filter',
    when(
        (col('filter') == 0)
        & (col('Service_Date') < "2024-12-04")
        & (F.min('source_members').over(window_spec) == "2024-12-04")
        & (col('unique_key') == F.min('unique_key').over(window_spec))
        , 1
    ).otherwise(col('filter'))
)'''


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''# Step 1: Identify groups where every B = 0
groups_with_all_zeros = (
    test.groupBy("Claim_Reference_Number")
    .agg(F.sum("filter").alias("sum_filter"))
    .filter(F.col("sum_filter") == 0)
    .select("Claim_Reference_Number")
)

# Step 2: Filter the original DataFrame
filtered_df = test.join(groups_with_all_zeros, on="Claim_Reference_Number", how="inner")

display(filtered_df)'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(filtered_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(test.filter(col('filter') == 1).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(test.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the window specification
window_spec = Window.partitionBy("Provider_ID").orderBy(F.desc("Service_Date"), F.desc('Claim_Date'), F.desc('Provider_Participation_Type'))

# Add the new column 'A' with the first value of column 'B' within the group sorted by 'D'
df_cl90 = df_cl90.withColumn("Current_Participation_Type", F.first(F.col("Provider_Participation_Type")).over(window_spec))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = Window.partitionBy('Member_ID').orderBy(F.desc('Service_Date'))

# Get the value of B where C is max in each group of D
df_cl90 = df_cl90.withColumn('Member_PT_Most_Recent', F.first('Member_PT').over(window_spec))
df_cl90 = df_cl90.withColumn('Age_Most_Recent', F.first('Age').over(window_spec))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cl90 = df_cl90.withColumn("Provider_Facility_UID", concat_ws("-", col("Provider_ID"), col("Facility_ID")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cl90 = df_cl90.join(df_co_pay_tier, on = "Member_ID", how = "left").select(df_cl90["*"], df_co_pay_tier["CRA_Co_Pay_Tier"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cl90.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/sunlife_gold/fact_claims")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CL92

# CELL ********************

df_cl92 = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.sunlife.sunlife_cl92')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# NEW - BY CLAIM DATE AND ADJ DATE

# Columns to be Matched
columns_to_match = ['Member_ID', 'Procedure_Code_Submitted', 'Tooth_Number', 'Tooth_Surface']

window_spec = Window.partitionBy("Member_ID","Procedure_Code_Submitted","Tooth_Number","Tooth_Surface").orderBy("Claim_Date")

df_with_duplicates = df_cl92.withColumn("row_num",when(col("Preauthorization_Indicator")=="Required",F.row_number().over(window_spec)))\
                    .withColumn("last_adj_date",F.lag("Adjudicated_Date").over(window_spec))
df_with_duplicates = df_with_duplicates.withColumn("duplicate", when((col("row_num")>1) & (col("Claim_Date") >= col("last_adj_date")),1).otherwise(0))


df_cl92 = df_with_duplicates.withColumn("Resubmission", when(col("duplicate") == 1 ,F.row_number().over(window_spec)-1).otherwise(0)).drop("row_num","duplicate","last_adj_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Resubmission column - flags when a claim has been resubmitted

window_spec = Window.partitionBy("Member_ID", "Procedure_Code_Submitted", "Tooth_Number", "Tooth_Surface").orderBy("Member_ID", "Adjudicated_Date")

df_cl92 = df_cl92.withColumn("row_num", F.row_number().over(window_spec))

df_cl92 = df_cl92.withColumn("Resubmission_Old", F.when(F.col("row_num") > 1, F.col("row_num") - 1).otherwise(0)).drop("row_num")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cl92 = df_cl92.withColumn(
    'Procedure_Group_Temp',
    F.when(
        (F.col('Procedure_Code_Submitted').startswith("213"))
        | (F.col('Procedure_Code_Submitted').startswith("236"))
        | (F.col('Procedure_Code_Submitted').startswith("257"))
        | (F.col('Procedure_Code_Submitted').startswith("272"))
        | (F.col('Procedure_Code_Submitted').startswith("273"))
        | (F.col('Procedure_Code_Submitted').startswith("296"))
        , "Crowns")
    .when(
        (F.col('Procedure_Code_Submitted').startswith("53301"))
        | (F.col('Procedure_Code_Submitted').startswith("53302"))
        | (F.col('Procedure_Code_Submitted').startswith("53304"))
        | (F.col('Procedure_Code_Submitted').startswith("52531"))
        | (F.col('Procedure_Code_Submitted').startswith("52532"))
        | (F.col('Procedure_Code_Submitted').startswith("52542"))
        | (F.col('Procedure_Code_Submitted').startswith("52543"))
        , "Complete & Partial Dentures")
    .when(
        (F.col('Procedure_Code_Submitted').startswith("41"))
        | (F.col('Procedure_Code_Submitted').startswith("52"))
        | (F.col('Procedure_Code_Submitted').startswith("53"))
        , "Partial Dentures")
    .when(
        (F.col('Procedure_Code_Submitted').startswith("31"))
        | (F.col('Procedure_Code_Submitted').startswith("51"))
        , "Complete Dentures")
    .when(
        (F.col('Procedure_Code_Submitted').startswith("424"))
        | (F.col('Procedure_Code_Submitted').startswith("42000"))
        | (F.col('Procedure_Code_Submitted').startswith("42001"))
        , "Root Planing")
    .when(
        (F.col('Procedure_Code_Submitted').startswith("11115"))
        | (F.col('Procedure_Code_Submitted').startswith("11116"))
        | (F.col('Procedure_Code_Submitted').startswith("11117"))
        | (F.col('Procedure_Code_Submitted').startswith("11119"))
        | (F.col('Procedure_Code_Submitted').startswith("43419"))
        | (F.col('Procedure_Code_Submitted').startswith("43405"))
        | (F.col('Procedure_Code_Submitted').startswith("43406"))
        , "Above Limits Scaling")
    .otherwise("Others")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

missing_info_codes = ['DY3', 'DCA', 'DY4', 'DNB', 'DY7', 'DCB', 'DCE', 'DYA', 'DNF', 'DY5', 'DG0', 'DCG', 'DZB', 'DU3', 'DT3', 'DU5', 'DNA', 'DM9', 'DNU', 'DNH', 'DNW', 'DNT', 'DNO', 'DC8', 'DBG', 'DY8', 'DNM', 'DN3', 'D4D', 'D5A', 'D5B', 'D5G', 'D5C', 'D5D', 'D5E', 'D5F', 'DNC', 'DNI', 'DNE', 'DNP', 'DNV', 'D36', 'DR4', 'DU7', 'DNL', 'DNK', 'DM8', 'D73', 'DU6', 'DG6', 'DNN', 'DN5', 'DM7', 'DNI', 'D37', 'D35', 'D66', 'DR2', 'DN2', 'DNJ', 'DT1', 'DL5', 'DC2', 'D54', 'D32']

df_cl92 = df_cl92.withColumn(
    'Reason_Code_Group',
    F.when(
        (F.col('Paid_Amount') > 0)
        , "Approved")
    .when(
        (F.col('Paid_Amount') == 0 )
        & ((F.col('Reason_Code').isin(missing_info_codes)) 
        | (F.col('Remark_Code').isin(missing_info_codes)))
        , "Incomplete preauthorization submission")
    .when(
        (F.col('Paid_Amount') == 0)
        , "Declined"
    )
    .otherwise("Other Reason")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

window_spec = Window.partitionBy('Member_ID').orderBy(F.desc('Service_Date'))

# Get the value of B where C is max in each group of D
df_cl92 = df_cl92.withColumn('Member_PT_Most_Recent', F.first('Member_PT').over(window_spec))
df_cl92 = df_cl92.withColumn('Age_Most_Recent', F.first('Age').over(window_spec))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cl92 = df_cl92.withColumn("Provider_Facility_UID", concat_ws("-", col("Provider_ID"), col("Facility_ID")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cl92 = df_cl92.join(df_co_pay_tier, on = "Member_ID", how = "left").select(df_cl92["*"], df_co_pay_tier["CRA_Co_Pay_Tier"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cl92.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/sunlife_gold/fact_estimates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Providers

# MARKDOWN ********************

# ### Reading source tables

# CELL ********************

# Reading CL90 with select columns related to providers and facility
df_cl90 = spark.sql(f'SELECT Provider_ID, Facility_ID, Facility_Postal_Code, Facility_FSA, Facility_PT, Current_Participation_Type, Specialty, Service_Date, Submitted_Date, COB_Amount, Paid_Amount FROM {gold_lakehouse_name}.sunlife_gold.fact_claims WHERE Specialty != "DENOFF"')

# Reading PP08
df_pp08 = spark.sql(f'SELECT * FROM {gold_lakehouse_name}.sunlife.sunlife_pp08')

# Reading Dental School mapping file
schema = StructType([
    StructField('Provider_ID',StringType(), True), 
    StructField('Dental_School',StringType(), True), 
    StructField('DS_Specialty',StringType(), True), 
    ])  
df_ds = spark.read.format("csv").option("header","true").schema(schema).load(f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}/Files/Mapping/Mapping_Dental_School_Providers.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Adding calculated columns to both tables

# CELL ********************

# CL90

# 1. Adding Unique ID (Provider ID - Facility ID)
df_cl90 = df_cl90.withColumn("Provider_Facility_UID",concat_ws("-", col("Provider_ID"), col("Facility_ID")))

# providing a rank of UID by Service Date - to drop older rows when selecting one row per provider
window_spec = Window.partitionBy("Provider_Facility_UID").orderBy(F.col("Service_Date").desc())
df_cl90_withRank = df_cl90.withColumn("Rank",F.rank().over(window_spec))
 
# 1. Participation type based on latest service date, 
# 2. last service date, 
# 3. participation date based on lowest submitted date, 
# 4. specialty is based on provider and facility id
# 5. paid status to check if the provider id was ever paid or not
# 6. Area type of the facility (Urban/Rural) based on facility FSA
df_cl90_withRank = df_cl90_withRank.withColumn("Last_Service_Date",F.max("Service_Date").over(Window.partitionBy("Provider_ID")))\
        .withColumn("Participation_Date",F.min(F.when(F.col("Submitted_Date")>F.lit("2024-07-07"),F.col("Submitted_Date")).otherwise(lit("2024-07-08").cast("date"))).over(Window.partitionBy("Provider_ID")))\
        .withColumn("Provider_Specialty",F.first(F.col("Specialty")).over(Window.partitionBy("Provider_ID","Facility_ID")))\
        .withColumn("Paid_Status",F.when((F.round(F.sum(F.col("Paid_Amount")).over(Window.partitionBy("Provider_ID")), 2)>0) | (F.round(F.sum(F.col("COB_Amount")).over(Window.partitionBy("Provider_ID")), 2)>0),"Y").otherwise("N"))\
        .withColumn("Facility_Area_Type",when(col('Facility_FSA').substr(2,1)=="0","Rural").otherwise("Urban"))\
        .withColumnRenamed("Current_Participation_Type","Participation_Type")
        #.withColumn("Participation_Type",F.expr("max_by(Provider_Participation_Type, Service_Date)").over(Window.partitionBy("Provider_ID")))\


# PP08

# 1. Adding facility id with "0" since doesnt exist in PP08 and will be required to merge for providers who have not made a claim yet
df_pp08 = df_pp08.withColumn("Facility_ID",lit("0"))\
        .withColumn("Provider_Facility_UID",concat_ws("-", col("Provider_ID"), col("Facility_ID")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Selecting latest row for a provider based on rank - window function
main_dim = df_cl90_withRank.filter(F.col("Rank")==1)\
    .select("Provider_Facility_UID","Provider_ID","Facility_ID","Facility_Postal_Code","Facility_FSA","Facility_PT",\
    "Facility_Area_Type","Participation_Type","Provider_Specialty","Participation_Date","Last_Service_Date","Paid_Status")

# Dropping duplicates in UID
main_dim = main_dim.drop_duplicates(['Provider_Facility_UID'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Extracting Provider Info from PP08
# ###### 1. Original participating date for enrolled providers
# ###### 2. Unique providers who are enrolled but have not made a claim yet

# CELL ********************

# 1. Joining main dim with pp08 df to get info for signed up providers
joined_df = main_dim.join(df_pp08, on=["Provider_ID"], how="left")\
        .select(main_dim["*"],df_pp08["Participating_Date"],df_pp08["Enrolled_Status"],df_pp08["Language"])


# 2. Anti join PP08 with main df to get providers who have not made a claim yet and renaming columns to match
anti_join_df = df_pp08.join(main_dim, on=["Provider_ID"], how="left_anti")\
        .drop("PT","Direct_Billing")\
        .withColumnsRenamed({"Specialty": "Provider_Specialty",\
                            "Provider_Postal_Code": "Facility_Postal_Code",\
                            "Provider_PT":"Facility_PT",\
                            "Provider_FSA":"Facility_FSA",\
                            "Provider_Area_Type":"Facility_Area_Type",\
                            "Source":"Dropout_Date"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Final dataframe
# ###### Adjusting values for some variables for the providers who have not made a claim from PP08 to the main df

# CELL ********************

# initializing final df with anti join and main df
final_dim = joined_df.unionByName(anti_join_df,allowMissingColumns=True)

# Adjusting participating date for signed up providers to reflect their original date from PP08
final_dim = final_dim.withColumn("Participating_Date",F.when((F.col("Participation_Type")=="Claim-by-Claim Provider"),F.col("Participation_Date"))
                                                        .when((F.col("Participation_Type")=="Participating Provider") & (F.col("Participating_Date").isNull()),F.col("Participation_Date"))
                                                        .otherwise(F.col("Participating_Date")))\
        .withColumn("Participation_Type",F.when((F.col("Enrolled_Status")=="Y") & (F.col("Participation_Type").isNull()),lit("Participating Provider"))
                                            .when((F.col("Enrolled_Status")=="N") & (F.col("Participation_Type").isNull()),lit("Unknown"))
                                            .otherwise(F.col("Participation_Type")))\
        .withColumn("Paid_Status",F.when((F.col("Participation_Type")=="Participating Provider") & (F.col("Paid_Status").isNull()),lit("N"))
                                        .when((F.col("Participation_Type")=="Unknown") & (F.col("Paid_Status").isNull()),lit("N"))
                                        .otherwise(F.col("Paid_Status")))\
        .drop("Participation_Date","Enrolled_Status","Provider_PT_Alt")

#display(final_dim)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Joining to get dental school columns from mapping table

# CELL ********************

# Joining with dental school mapping table
df_providers = final_dim.join(df_ds, on=["Provider_ID"], how="left")
#display(df_providers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Adjusting full names for PT and Specialty from PP08 join to abbreaviations

# CELL ********************

# Creating a list dictionary for the Specialty Names
SpecDic = {'General Practitioner':'GP','Dental Hygienist':'HY','Denturist':'DT','Endodontist':'EN','Oral Pathologist':'OP','Oral Radiologist':'OR','Oral & Maxillofacial Surgeon':'OS',\
    'Orthodontist':'OT','Pediatric Dentist':'PD','Periodontist':'PE','Prosthodontist':'PR','Oral Medicine':'OM','Anaesthesiologist':'AN','Dental School':'DS','Dental Office':'DENOFF'}

# Replacing Abbrevation Values using the dictionary created
df_providers = df_providers.na.replace(SpecDic, subset=["Provider_Specialty"])

#df_providers.groupBy("Provider_Specialty").count().collect()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating a list dictionary for the provinces and territories

ProvinceDic = {'Alberta':'AB','British Columbia':'BC','Manitoba':'MB','New Brunswick':'NB','Newfoundland and Labrador':'NL','Nova Scotia':'NS','Northwest Territories':'NT','Nunavut':'NU','Ontario':'ON','Prince Edward Island':'PE','Quebec':'QC','Saskatchewan':'SK','Yukon':'YT'}


# Replacing abbrevation values using the dictionary created
df_providers = df_providers.na.replace(ProvinceDic, subset=["Facility_PT"])



# df_providers.groupBy("Facility_PT").count().collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Adding Connector columns for Billing universe and FI02

# CELL ********************

df_providers = df_providers.withColumn("Billing_Join",concat_ws("-", col("Facility_FSA"), col("Provider_Specialty")))\
                        .withColumn("Provider_Validity",when((F.col("Participation_Type")=="Participating Provider") | (F.col("Participation_Type")=="Claim-by-Claim Provider"),lit("True").cast('boolean')).otherwise(lit("False").cast('boolean')))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Validating Numbers

# CELL ********************

#print(f"Unique in pp : {df_providers.select('Provider_ID').where((col('Participation_Type')=='Participating Provider')).distinct().count()}")
#print(f"Unique in cbc : {df_providers.select('Provider_ID').where((col('Participation_Type')=='Claim-by-Claim Provider') & (col('Paid_Status')=='Y')).distinct().count()}")
# Checking if there is any duplication in new id
df_providers \
    .groupby(['Provider_Facility_UID']) \
    .count() \
    .where('count > 1') \
    .sort('count', ascending=False) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dropping duplicate UID
df_providers = df_providers.drop_duplicates(["Provider_Facility_UID"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Temp FR variables

# CELL ********************

df_providers = df_providers.withColumn(
        'Language_FR',
        when(col('Language') == lit("English"), lit("Anglais"))
        .when(col('Language') == lit("French"), lit("Français"))
    ).withColumn(
        'Facility_Area_Type_FR',
        when(col('Facility_Area_Type') == lit("Urban"), lit("urbaine"))
        .when(col('Facility_Area_Type') == lit("Rural"), lit("rurale"))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### Enrolled Status Column

# CELL ********************

df_providers = df_providers.withColumn(
    'Enrolled_Status',
    when(col('Participation_Type') == "Participating Provider", True)
    .otherwise(False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Output table

# CELL ********************

# Overwriting the transformed dataframe into delta table
df_providers.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/sunlife_gold/dim_providers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
