# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9b0ffb12-995f-4c54-8fec-80e863a21b73",
# META       "default_lakehouse_name": "Data_Quality_Lakehouse_DEV",
# META       "default_lakehouse_workspace_id": "4748fbe5-9b18-4aac-9d74-f79c39ff81db",
# META       "known_lakehouses": [
# META         {
# META           "id": "9b0ffb12-995f-4c54-8fec-80e863a21b73"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Setup

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

# Possible options: "initial", "update"
runType = "initial"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Notebooks

# CELL ********************

%run 00_Load_Packages

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ETL_Helpers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Mount Lakehouses

# CELL ********************

workspace_id = spark.conf.get("trident.workspace.id")
bronze_lakehouse_name, bronze_mount_paths = mount_lakehouse("Bronze_Lakehouse", workspace_id)
dq_lakehouse_name, dq_mount_paths = mount_lakehouse("Data_Quality_Lakehouse", workspace_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Assign Sources

# CELL ********************

overwrite_1_list = [
    "cra/copay_tiers",
    "esdc/contact_centre_stats",
    "esdc/fsa_applications",
    "esdc/pt_applications",
    "esdc/fsa_enrolled",
    "esdc/pt_enrolled",
    "hc/cost_of_administration",
    "hc/cost_of_sunlife_contract",
    "hc/eligible_population_estimates",
    "hc/procedure_codes",
    "hc/sunlife_contract_milestones",
    #"ohds/grouping",
    #"ohds/mapping",
    "statcan/pccf",
    "sunlife/cards_mailout",
    #"sunlife/fee_guide_universe",
    #"sunlife/overpayments_universe",
    "sunlife/pp08",
    "sunlife/procedure_code_descriptions",
    "sunlife/provider_billing",
    #"sunlife/providers_universe",
    "sunlife/reason_and_remark_codes",
]

append_1_list = [
    #"hc/oh_ip/change_request",
    #"hc/oh_ip/financial_actuals",
    #"hc/oh_ip/financial_estimates",
    #"hc/oh_ip/risk",
    #"hc/oh_ip/schedule_baseline",
    #"hc/oh_ip/schedule_revised",
    "sunlife/contact_centre_stats",
    "sunlife/fi02",
    #"sunlife/financial_universe",
    #"sunlife/members_universe"
]

append_2_list = [
    "esdc/members_eligible",
    #"esdc/members_ineligible",
    "sunlife/cl90",
    "sunlife/cl92",
    #"sunlife/claims_universe",
    #"sunlife/estimates_universe"
]

all_source_list = overwrite_1_list + append_1_list + append_2_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_schema = StructType([  
    StructField('full_path', StringType(), True),
    StructField('source', StringType(), True),
    StructField('file_name', StringType(), True),
    StructField('file_type', StringType(), True),
    StructField('priority', IntegerType(), True),
    StructField('write_behaviour', StringType(), True),
    StructField('source_date', DateType(), True),
    StructField('modified_date', DateType(), True),
    StructField('processed_status', BooleanType(), True),
    StructField('processed_date', DateType(), True)
]) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read Metadata

# CELL ********************

df_new = list_files(f"{bronze_mount_paths['fs_path']}/Files", all_source_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if runType == "initial":

    spark_df = spark.createDataFrame(df_new, write_schema)
    spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{dq_mount_paths['file_path']}/Tables/data_sources/data_sources")
    
    notebookutils.notebook.exit("Initial Run Complete")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_old = spark.sql(f"SELECT * FROM {dq_lakehouse_name}.data_sources.data_sources").toPandas()
df_old["source_date"] = pd.to_datetime(df_old["source_date"], errors='coerce')
df_old["processed_date"] = pd.to_datetime(df_old["processed_date"], errors='coerce')
df_old["modified_date"] = pd.to_datetime(df_old["modified_date"], errors='coerce')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Compare new and old DataFrames

# CELL ********************

df_joined = df_new.merge(df_old, how = 'outer', on = 'full_path', suffixes = ('', '_2'), indicator = True)

df_joined = df_joined.loc[df_joined['_merge'] != "right_only"]

df_joined['left_only'] = df_joined['_merge'] == 'left_only'
df_joined['not_processed'] = df_joined['processed_status_2'] == False
df_joined['modified_overwrite'] = (
    (df_joined['write_behaviour'] == "overwrite")
    & (df_joined['modified_date'] > df_joined['modified_date_2'])
)

df_joined['include'] = (
    (df_joined['left_only'] == True)
    | (df_joined['not_processed'] == True)
    | (df_joined['modified_overwrite'] == True)
)

df_joined['processed_status'] = np.where(df_joined['include'] == False, df_joined['processed_status_2'], df_joined['processed_status'])
df_joined['processed_date'] = np.where(df_joined['include'] == False, df_joined['processed_date_2'], df_joined['processed_date'])

df_joined = df_joined.drop(columns = [col for col in df_joined.columns if col.endswith('_2') or col == '_merge'], axis = 1)
df_joined = df_joined.sort_values(by = ['priority', 'write_behaviour', 'source', 'source_date'])

new_entries = df_joined.loc[df_joined['include'] == True]

df_joined = df_joined.drop(columns = ['left_only', 'not_processed', 'modified_overwrite', 'include'], axis = 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write to Delta Table in Data Quality Lakehouse

# CELL ********************

spark_df = spark.createDataFrame(df_joined, write_schema)
spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{dq_mount_paths['file_path']}/Tables/data_sources/data_sources")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Exit Notebook with new entries

# CELL ********************

new_entries['source_date'] = pd.to_datetime(new_entries['source_date']).dt.strftime('%Y-%m-%d')
new_entries['modified_date'] = pd.to_datetime(new_entries['modified_date']).dt.strftime('%Y-%m-%d')
new_entries['processed_date'] = pd.to_datetime(new_entries['processed_date']).dt.strftime('%Y-%m-%d')

sources_priority_1 = new_entries.loc[new_entries['priority'] == 1]['source'].unique().tolist()
sources_priority_2 = new_entries.loc[new_entries['priority'] == 2]['source'].unique().tolist()

files_priority_1 = new_entries.loc[new_entries['priority'] == 1].to_dict(orient="records")
files_priority_2 = new_entries.loc[new_entries['priority'] == 2].to_dict(orient="records")

new_entries_count = len(new_entries.index)
action_needed = 1 if new_entries_count > 0 else 0

result = {
    "new_entries": new_entries_count,
    "action_needed": action_needed,
    "sources_priority_1": sources_priority_1,
    "sources_priority_2": sources_priority_2,
    "files_priority_1": files_priority_1,
    "files_priority_2": files_priority_2
}

mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
