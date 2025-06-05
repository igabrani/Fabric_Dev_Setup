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
# META           "id": "787f65b7-02b8-4b8c-930e-3405f9a89660"
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

# PARAMETERS CELL ********************

files_priority_1_str = ""
files_priority_2_str = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files_priority_1 = ast.literal_eval(files_priority_1_str)
files_priority_2 = ast.literal_eval(files_priority_2_str)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mount bronze lakehouse
workspace_id = spark.conf.get("trident.workspace.id")

if workspace_id == "4748fbe5-9b18-4aac-9d74-f79c39ff81db": #dev
    bronze_lakehouse_id = "b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe"
    silver_lakehouse_name = "Silver_Lakehouse_DEV"
    data_quality_lakehouse_name = "Data_Quality_Lakehouse_DEV"

elif workspace_id == "ec6eb7c4-c224-4908-8c57-cd348b9f58f4": #test
    bronze_lakehouse_id = "c2b72635-1f18-4f8b-909a-94726216cc87"
    silver_lakehouse_name = "Silver_Lakehouse_TEST"
    data_quality_lakehouse_name = "Data_Quality_Lakehouse_TEST"

elif workspace_id == "3eb968ef-6205-4452-940d-9a6dcbf377f2": #prod
    bronze_lakehouse_id = "dee2baf0-f8c0-4682-9154-bf0a213e41ee"
    silver_lakehouse_name = "Silver_Lakehouse_PROD"
    data_quality_lakehouse_name = "Data_Quality_Lakehouse_PROD"

notebookutils.fs.mount(f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}", "/lakehouse/Bronze_Lakehouse")

mount_path = get_mount_path(data_quality_lakehouse_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_sources = spark.sql(f"SELECT * FROM {data_quality_lakehouse_name}.dbo.data_sources").orderBy(['priority', 'write_behaviour', 'source', 'source_date'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

files = []

for file in files_priority_1 + files_priority_2:
    files.append(file['full_path'])

data_sources_update = data_sources.withColumn(
    'processed_status',
    F.when(col('full_path').isin(files), True)
    .otherwise(col('processed_status'))
)


data_sources_update = data_sources_update.withColumn(
    'processed_date',
    F.when(col('full_path').isin(files), F.current_date())
    .otherwise(col('processed_date'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(data_sources_update)
#display(data_sources)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_sources_update.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{mount_path}/Tables/data_sources")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
