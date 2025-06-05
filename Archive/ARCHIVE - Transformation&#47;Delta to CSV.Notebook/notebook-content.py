# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe",
# META       "default_lakehouse_name": "Bronze_Lakehouse_DEV",
# META       "default_lakehouse_workspace_id": "4748fbe5-9b18-4aac-9d74-f79c39ff81db",
# META       "known_lakehouses": [
# META         {
# META           "id": "b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


delta_input_path = "abfss://ec6eb7c4-c224-4908-8c57-cd348b9f58f4@onelake.dfs.fabric.microsoft.com/c2b72635-1f18-4f8b-909a-94726216cc87/Tables/dbo/Query_Test"
delta_output_path = "abfss://ec6eb7c4-c224-4908-8c57-cd348b9f58f4@onelake.dfs.fabric.microsoft.com/c2b72635-1f18-4f8b-909a-94726216cc87/Tables/dbo/Transformed_Query_Test"
csv_output_path = "abfss://ec6eb7c4-c224-4908-8c57-cd348b9f58f4@onelake.dfs.fabric.microsoft.com/c2b72635-1f18-4f8b-909a-94726216cc87/Files/Power Automate Test"

df = spark.read.format("delta").load(delta_input_path)

# 
df = df.drop("H")  # Remove the "H" column 
df = df.withColumn("Provider_Number", col("Provider_Number").cast("bigint")) \
       .withColumn("Provider_Facility_ID", col("Provider_Facility_ID").cast("bigint")) \
       .withColumn("Member_ID", col("Member_ID").cast("bigint")) \
       .withColumn("Billing_Group", col("Billing_Group").cast("int")) \
       .withColumn("CFR_Date", col("CFR_Date").cast("date")) \
       .withColumn("Adjudicated_Date", col("Adjudicated_Date").cast("date")) \
       .withColumn("EFT", col("EFT").cast("double")) \
       .withColumn("Cheques", col("Cheques").cast("double")) \
       .withColumn("Payment_Cancellations_EFT", col("Payment_Cancellations_EFT").cast("bigint")) \
       .withColumn("Payment_Cancellations_Cheque", col("Payment_Cancellations_Cheque").cast("bigint")) \
       .withColumn("Refunds_EFT", col("Refunds_EFT").cast("bigint")) \
       .withColumn("Refunds_Cheque", col("Refunds_Cheque").cast("bigint")) \
       .withColumn("Other_Adjustments", col("Other_Adjustments").cast("double")) \
       .withColumn("Voids_EFT", col("Voids_EFT").cast("bigint")) \
       .withColumn("Voids_Cheque", col("Voids_Cheque").cast("bigint"))


# Write transformed data back to Delta format in Fabric Lakehouse
df.write.format("delta").mode("overwrite").save(delta_output_path)

# Save a CSV copy for data fall back
df.write.mode("overwrite").option("header", "true").csv(csv_output_path)

print("Data transformation complete and saved to Delta format & CSV.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
