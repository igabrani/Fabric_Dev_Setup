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

from pyspark.sql.functions import when, col, lit, concat_ws, isnull
import numpy as np

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

proc_codes = spark.sql("SELECT * FROM Gold_Lakehouse_TEST.hc.procedure_codes")


cl90 = spark.sql("SELECT * FROM Gold_Lakehouse_TEST.sunlife_gold.cl90 WHERE Specialty != 'DENOFF' AND Paid_Amount > 0")
cl90 = cl90.withColumnRenamed("Code_Join_Paid","Code_Join")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

map = spark.read.format("csv").option("header","true").load("Files/Procedure_Codes/procedure_code_mapping.csv")
display(map)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

code_map_join = cl90.join(map, cl90.Procedure_Code_Paid == map.Dummy_Code, how="left")

code_map_result = code_map_join.withColumn("Procedure_Code_Paid",
    when(col("Procedure_Code").isNotNull(), col("Procedure_Code")).otherwise(col("Procedure_Code_Paid")))\
    .drop("Dummy_Code", "Procedure_Code")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cl90 = cl90.withColumn(
    "Code_Join_New",
    when(
        col("Procedure_Code_Paid").isNull(),
        None
    ).when(
        col("Specialty").isin("DT", "HY"),
        concat_ws("-", col("Procedure_Code_Paid"), col("Specialty"))
    ).when(
        (col("QC_Flag") == "QC") | col("Specialty").isin("OS"),
        concat_ws("-", col("Procedure_Code_Paid"), col("QC_Flag"), col("Specialty"))
    ).otherwise(
        concat_ws("-", col("Procedure_Code_Paid"), col("QC_Flag"))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined = cl90.join(proc_codes, cl90.Code_Join_New == proc_codes.Code_Join , how="left_anti")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

codes = joined.select("Code_Join","Procedure_Code_Paid").drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"all codes in cl90: {cl90.select('Procedure_Code_Paid').distinct().count()}")
print(f"CL90 codes didnt join ABCD: {codes.select('Procedure_Code_Paid').distinct().count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

codes.write.options(header='True', delimiter=',') \
 .csv("Files/Procedure_Codes/CL90_missing_codes_2025_04_16.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
