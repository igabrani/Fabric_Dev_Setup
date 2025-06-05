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
# META           "id": "911ba9c9-2599-42c5-b72c-30050958d181"
# META         },
# META         {
# META           "id": "787f65b7-02b8-4b8c-930e-3405f9a89660"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df = spark.sql("SELECT * FROM CDCP.Silver_Lakehouse_PROD.sunlife.sunlife_pp08")
#display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save('Tables/sunlife/sunlife_pp08')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
