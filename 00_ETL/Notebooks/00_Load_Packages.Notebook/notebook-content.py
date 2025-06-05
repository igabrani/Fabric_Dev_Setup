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

# # Load Packages

# CELL ********************

# Python
import pandas as pd
import numpy as np
import os
import time
import glob
import re
import json
import ast
from datetime import datetime, timedelta

# PySpark
from delta import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, IntegerType, LongType, BooleanType, ArrayType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import first, col, substring, to_date, date_format, regexp_extract, when, length, expr, lpad, lit, concat_ws, countDistinct, max

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
