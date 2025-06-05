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

# MARKDOWN ********************

# # Setup

# MARKDOWN ********************

# ## Create Parameters

# PARAMETERS CELL ********************

measure_table = "yes"
date_table = "yes"
grouping_tables = "yes"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Packages

# CELL ********************

%run 00_Load_Packages


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Measure Table

# CELL ********************

if measure_table == "yes":
        
    data = [(1, 'MeasureTable')]
    columns = ['ID', 'Col1']

    measure_df = spark.createDataFrame(data, columns)
    measure_df.show()
    measure_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/auxiliary/_measure_table")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Date Table

# MARKDOWN ********************

# ## Calendar Creation

# CELL ********************

# define boundaries
startdate = datetime.strptime('2023-01-01','%Y-%m-%d')
enddate   = (datetime.now() + timedelta(days=365 * 2)).replace(month=12, day=31)  # datetime.strptime('2023-10-01','%Y-%m-%d')

# COMMAND ----------

# define column names and its transformation rules on the Date column
column_rule_df = spark.createDataFrame([
    ("Date_ID",                 "cast(date_format(date, 'yyyyMMdd') as int)"),           # 20230101
    ("Year",                    "year(date)"),                                           # 2023
    ("Fiscal_Quarter",          "(quarter(date) + 2) % 4 + 1"),                          # 4
    ("Quarter",                 "quarter(date)"),                                        # 1
    ("Month",                   "month(date)"),                                          # 1
    ("Day",                     "day(date)"),                                            # 1
    ("Week",                    "weekofyear(date)"),                                     # 52 (start of week is Monday)
    ("Year_Start",              "to_date(date_trunc('year', date))"),                    # 2023-01-01
    ("Year_End",                "last_day(date_add(date_trunc('year', date), 360))"),    # 2023-12-31
    ("Quarter_Start",           "to_date(date_trunc('quarter', date))"),                 # 2023-01-01
    ("Quarter_End",             "last_day(date_add(date_trunc('quarter', date), 80))"),  # 2023-03-31
    ("Month_Start",             "to_date(date_trunc('month', date))"),                   # 2023-01-01
    ("Month_End",               "last_day(date)"),                                       # 2023-01-31
    ("Week_Start",              "to_date(date_trunc('week', date))"),                    # 2023-01-01
    ("Week_End",                "to_date(date_add(date_trunc('week', date), 6))"),       # 2023-01-07
    ("Year_Month_String",       "date_format(date, 'yyyy-MM')"),                         # 2023-01
    ("Quarter_Name_Long",       "date_format(date, 'QQQQ')"),                            # 1st quarter
    ("Quarter_Name_Short",      "date_format(date, 'QQQ')"),                             # Q1
    ("Quarter_Number_String",   "date_format(date, 'QQ')"),                              # 01
    ("Month_Name_Long",         "date_format(date, 'MMMM')"),                            # January
    ("Month_Name_Short",        "date_format(date, 'MMM')"),                             # Jan
    ("Month_Number_String",     "date_format(date, 'MM')"),                              # 01
    ("Week_Name_Long",          "concat('week', lpad(weekofyear(date), 2, '0'))"),       # week 01
    ("Week_Name_Short",         "concat('w', lpad(weekofyear(date), 2, '0'))"),          # w01
    ("Week_Number_String",      "lpad(weekofyear(date), 2, '0')"),                       # 01
    ("Day_Number_String",       "date_format(date, 'dd')"),                              # 01
    ("Day_Of_Week",             "weekday(date) + 1"),                                    # 7
    ("Day_Of_Week_Name_Long",   "date_format(date, 'EEEE')"),                            # Sunday
    ("Day_Of_Week_Name_Short",  "date_format(date, 'EEE')"),                             # Sun
    ("Day_Of_Month",            "cast(date_format(date, 'd') as int)"),                  # 1
    ("Day_Of_Year",             "cast(date_format(date, 'D') as int)"),                  # 1
], ["new_column_name", "expression"])


# COMMAND ----------

# explode dates between the defined boundaries into one column
start = int(startdate.timestamp())
stop  = int(enddate.timestamp())
dim_date = spark.range(start, stop, 60*60*24).select(col("id").cast("timestamp").cast("date").alias("Date"))

# COMMAND ----------

# this loops over all rules defined in column_rule_df adding the new columns
for row in column_rule_df.collect():
    new_column_name = row["new_column_name"]
    expression = F.expr(row["expression"])
    dim_date = dim_date.withColumn(new_column_name, expression)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Friday Week Columns


# CELL ********************

dim_date = dim_date.withColumn("Year_Number", F.col("year") - 2023)

dim_date = dim_date.withColumn(
    "adjusted_date",
    F.date_sub(F.col("date"), (F.dayofweek("date") + 1) % 7)
)

# Calculate the week number based on the adjusted date
dim_date = dim_date.withColumn("Week_Friday_Start", F.weekofyear("adjusted_date"))

dim_date = dim_date.withColumn(
    "Week_Friday_Start",
    F.when(
        (col("Day_Of_Year") < 7) & (F.col("Week_Friday_Start") == 52),
        0
    ).otherwise(F.col("Week_Friday_Start"))
)

dim_date = dim_date.withColumn(
    "Week_Friday_Start_With_Year_Number",
    col("Year_Number") * 52 + col("Week_Friday_Start")
)
# Drop the intermediate column if not needed
dim_date = dim_date.drop("adjusted_date")

display(dim_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fiscal Year Column

# CELL ********************

dim_date = dim_date.withColumn("Fiscal_Year", 
                  F.when(F.month(col("Date")) >= 4, F.concat(F.lit('FY'),col("Year").cast('string'),F.lit('-'), (col("Year")+1).cast('string')))
                  .otherwise(F.concat(F.lit('FY'),(col("Year")-1).cast('string'),F.lit('-'), col("Year").cast('string'))))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Holiday List

# CELL ********************

holidays = [#2023											
            "2023-01-02" , "2023-04-07" , "2023-04-10" , "2023-05-22" , "2023-07-03" , "2023-08-07" , "2023-09-04" , "2023-10-02" , "2023-10-09" , "2023-11-13" , "2023-12-25" , "2023-12-26",

            #2024											
            "2024-01-01" , "2024-03-29" , "2024-04-01" , "2024-05-20" , "2024-07-01" , "2024-08-05" , "2024-09-02" , "2024-09-30" , "2024-10-14" , "2024-11-11" , "2024-12-25" , "2024-12-26",

            #2025											
            "2025-01-01" , "2025-04-18" , "2025-04-21" , "2025-05-19" , "2025-07-01" , "2025-08-04" , "2025-09-01" , "2025-09-30" , "2025-10-13" , "2025-11-11" , "2025-12-25" , "2025-12-26",
            
            #2026											
            "2026-01-01" , "2026-04-03" , "2026-04-06" , "2026-05-18" , "2026-07-01" , "2026-08-03" , "2026-09-07" , "2026-09-30" , "2026-10-12" , "2026-11-11" , "2026-12-25" , "2026-12-28",
            
            #2027											
            "2027-01-01" , "2027-03-26" , "2027-03-29" , "2027-05-24" , "2027-07-01" , "2027-08-02" , "2027-09-06" , "2027-09-30" , "2027-10-11" , "2027-11-11" , "2027-12-27" , "2027-12-28",
            
            #2028											
            "2028-01-03" , "2028-04-14" , "2028-04-17" , "2028-05-22" , "2028-07-03" , "2028-08-07" , "2028-09-04" , "2028-10-02" , "2028-10-09" , "2028-11-13" , "2028-12-25" , "2028-12-26",
            
            #2029											
            "2029-01-01" , "2029-03-30" , "2029-04-02" , "2029-05-21" , "2029-07-02" , "2029-08-06" , "2029-09-03" , "2029-10-01" , "2029-10-08" , "2029-11-12" , "2029-12-25" , "2029-12-26",
            
            #2030											
            "2030-01-01" , "2030-04-19" , "2030-04-22" , "2030-05-20" , "2030-07-01" , "2030-08-05" , "2030-09-02" , "2030-09-30" , "2030-10-14" , "2030-11-11" , "2030-12-25" , "2030-12-26"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Holidays Flag

# CELL ********************

dim_date = dim_date.withColumn("Holiday", F.when(col("Date").isin(holidays), F.lit(1)).otherwise(F.lit(0)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Holidays and Weekends Flag

# CELL ********************

dim_date = dim_date.withColumn("Include_Days", 
                   F.when((col("Date").isin(holidays)) | 
                        (col("Day_Of_Week_Name_Long").isin(["Saturday", "Sunday"])), F.lit(0))
                   .otherwise(F.lit(1))).dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Publish to Lakehouse

# CELL ********************

if date_table == "yes":

    dim_date.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/auxiliary/date_table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Grouping Tables

# CELL ********************

if grouping_tables == "yes":

    # Path to the folder containing CSV files
    folder_path = "/lakehouse/default/Files/ohds/grouping"

    # Loop over all files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):

            file_path = os.path.join(folder_path, filename)
            delta_table_path = f"Tables/grouping/{filename.split('.')[0].lower()}"

            df = pd.read_csv(file_path)
            spark_df = spark.createDataFrame(df)

            if filename == "preauth_decline_codes.csv":
                spark_df = spark_df.withColumn("Procedure_Code", spark_df['Procedure_Code'].cast(StringType()))

            spark_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(delta_table_path)

            print(f"Processed {filename} and saved to {delta_table_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
