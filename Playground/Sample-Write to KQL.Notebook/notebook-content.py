# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Sample Notebook: Write to Eventhouse
# ## How to write a dataframe to event house
# 
# Get the cluster url from the event house Query URI 


# MARKDOWN ********************

# ### Generate random data in a dataframe

# CELL ********************

from pyspark.sql.functions import rand, randn
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

# Number of rows you want
num_rows = 100

# Create a DataFrame with a range of numbers
df = spark.range(0, num_rows).withColumnRenamed("id", "row_id")

# Add random columns: uniform, normal, and a random integer [0, 99]
df = (df
      .withColumn("random_uniform", rand())
      .withColumn("random_normal", randn())
      .withColumn("random_int", (F.rand() * 100).cast(IntegerType()))
     )

df.show(10)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# The query URI for reading the data e.g. https://<>.kusto.data.microsoft.com.
kustoUri = "https://trd-au58pnjgtdtk304kxd.z6.kusto.fabric.microsoft.com"
# The database with data to be read.
database = "Logging_Eventhouse_DEV"
kustoTable = "test_table"
# The access credentials.
access_token = mssparkutils.credentials.getToken(kustoUri)
kustoOptions = {"kustoCluster": kustoUri, "kustoDatabase" :database, "kustoTable" : kustoTable }




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write data to a table in Eventhouse
df.write. \
format("com.microsoft.kusto.spark.synapse.datasource"). \
option("kustoCluster",kustoOptions["kustoCluster"]). \
option("kustoDatabase",kustoOptions["kustoDatabase"]). \
option("kustoTable", kustoOptions["kustoTable"]). \
option("accessToken", access_token). \
option("tableCreateOptions", "CreateIfNotExist").\
mode("Append"). \
save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
