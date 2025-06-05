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

# MARKDOWN ********************

# # Setup

# CELL ********************

# Mount bronze lakehouse
workspace_id = spark.conf.get("trident.workspace.id")

if workspace_id == "4748fbe5-9b18-4aac-9d74-f79c39ff81db": #dev
    bronze_lakehouse_name = "Bronze_Lakehouse_DEV"

elif workspace_id == "ec6eb7c4-c224-4908-8c57-cd348b9f58f4": #test
    bronze_lakehouse_name = "Bronze_Lakehouse_TEST"

elif workspace_id == "3eb968ef-6205-4452-940d-9a6dcbf377f2": #prod
    bronze_lakehouse_name = "Bronze_Lakehouse_PROD"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run 00_Load_Packages

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read Data

# CELL ********************

df_cdrs = spark.sql(f'SELECT * FROM {bronze_lakehouse_name}.dbo.cdrs')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cdrs = spark.sql(f'SELECT * FROM {bronze_lakehouse_name}.dbo.cdrs')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Clean column names
def clean_column_name(name):
    name = re.sub(r"[?/\-]", "", name)
    name = re.sub(r"\s+", "_", name)
    return name

# Step 2: Apply cleaned names
new_column_names = [clean_column_name(col) for col in df_cdrs.columns]
df_transformed = df_cdrs.toDF(*new_column_names)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Write Data

# CELL ********************

df_transformed.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/cdrs/cdrs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
