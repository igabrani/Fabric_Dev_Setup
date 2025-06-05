# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9fce90e6-f1fb-4f72-a38d-4cb8a6dcfa50",
# META       "default_lakehouse_name": "Silver_Lakehouse_TEST",
# META       "default_lakehouse_workspace_id": "ec6eb7c4-c224-4908-8c57-cd348b9f58f4",
# META       "known_lakehouses": [
# META         {
# META           "id": "9fce90e6-f1fb-4f72-a38d-4cb8a6dcfa50"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Import necessary Python packages
# Import all Python packages that will be used in this notebook

# CELL ********************

import pandas as pd
import pyspark.sql

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fetch DBOs
# Get all entities from the System Table related to the data security log system such as:
# - Users / Groups
# - Data / Data Products

# CELL ********************

users = spark.sql('SELECT * FROM sys.users')
print(users)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
