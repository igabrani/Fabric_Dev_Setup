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
# META       "default_lakehouse_workspace_id": "ec6eb7c4-c224-4908-8c57-cd348b9f58f4"
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

# CELL ********************

# Mount bronze lakehouse
workspace_id = spark.conf.get("trident.workspace.id")

if workspace_id == "4748fbe5-9b18-4aac-9d74-f79c39ff81db": #dev
    bronze_lakehouse_id = "b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe"
    silver_lakehouse_name = "Silver_Lakehouse_DEV"

elif workspace_id == "ec6eb7c4-c224-4908-8c57-cd348b9f58f4": #test
    bronze_lakehouse_id = "c2b72635-1f18-4f8b-909a-94726216cc87"
    silver_lakehouse_name = "Silver_Lakehouse_TEST"

elif workspace_id == "3eb968ef-6205-4452-940d-9a6dcbf377f2": #prod
    bronze_lakehouse_id = "dee2baf0-f8c0-4682-9154-bf0a213e41ee"
    silver_lakehouse_name = "Silver_Lakehouse_PROD"

notebookutils.fs.mount(f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}", "/lakehouse/Bronze_Lakehouse")


rootRead = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}/Files/HC/OH_IP/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Risk

# CELL ********************

schema = StructType([
    StructField('ID' ,LongType(), True), 
    StructField('Title', StringType(), True),
    StructField('Status', StringType(), True),
    StructField('Probability', LongType(), True), 
    StructField('Impact', LongType(), True),
    StructField('Priority_Ranking', StringType(), True),
    StructField('Date_Reported', DateType(), True),
    StructField('Copy_Date', DateType(), True),
    StructField('Type', StringType(), True),
    StructField('Score', DoubleType(), True),
    StructField('Expiration_Date', DateType(), True),
    StructField('Assigned_To', StringType(), True),
    StructField('Description', StringType(), True)
])

risk = spark.read.format("csv") \
    .option("header","true") \
    .option("multiline","true") \
    .option("escape","/") \
    .schema(schema) \
    .load(rootRead + "risk")

risk = risk.withColumn('Source', regexp_extract(F.input_file_name(), ".*/([^/]+\\.csv)", 1)) \
    .withColumn('Source_Date', to_date(regexp_extract(col('Source'), r"(\d{4}-\d{2}-\d{2})", 1), 'yyyy-MM-dd'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(risk)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

risk.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/oh_ip/risk")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Change Request

# CELL ********************

schema = StructType([
    StructField('ID', LongType(), True), 
    StructField('Title', StringType(), True), 
    StructField('Change_Request', StringType(), True), 
    StructField('Date_Submitted', DateType(), True),
    StructField('Approval_Date', DateType(), True),
    StructField('Support_Material', StringType(), True),
    StructField('Copy_Date', DateType(), True),
    StructField('PreventEmpty', StringType(), True)
])

change_request = spark.read.format("csv") \
    .option("header","true") \
    .option("multiline","true") \
    .option("quote","\"") \
    .option("escape","/") \
    .option("delimiter",",") \
    .schema(schema) \
    .load(rootRead + "change_request")

change_request = change_request.withColumn('Source', regexp_extract(F.input_file_name(), ".*/([^/]+\\.csv)", 1)) \
    .withColumn('Source_Date', to_date(regexp_extract(col('Source'), r"(\d{4}-\d{2}-\d{2})", 1), 'yyyy-MM-dd'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(change_request)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

change_request.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/oh_ip/change_request")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schedule Revised

# CELL ********************

schema = StructType([
    StructField('Task_number', LongType(), True), 
    StructField('Outline_number', StringType(), True), 
    StructField('Name', StringType(), True), 
    StructField('Duration_days', StringType(), True),
    StructField('Start', StringType(), True),
    StructField('Finish', StringType(), True),
    StructField('Depends_on', StringType(), True),
    StructField('Dependents_after', StringType(), True),
    StructField('Percent_complete', DoubleType(), True),
    StructField('Notes', StringType(), True),
    StructField('Assigned_to', StringType(), True),
    StructField('Bucket', StringType(), True),
    StructField('Effort', StringType(), True),
    StructField('Effort_completed_hours', StringType(), True),
    StructField('Effort_remaining_hours', StringType(), True),
    StructField('Milestone', StringType(), True),
    StructField('Notes_1', StringType(), True),
    StructField('Completed', StringType(), True),
    StructField('Checklist_items', StringType(), True),
    StructField('Labels', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('Sprint', StringType(), True),
    StructField('Goal', StringType(), True),
])    

schedule_revised = spark.read.format("csv") \
    .option("header","true") \
    .schema(schema) \
    .load(rootRead + "schedule_revised")

schedule_revised = schedule_revised.withColumn('Source', regexp_extract(F.input_file_name(), ".*/([^/]+\\.csv)", 1)) \
    .withColumn('Source_Date', to_date(regexp_extract(col('Source'), r"(\d{4}_\d{2}_\d{2})", 1), 'yyyy_MM_dd')) \
    .withColumn("Start", to_date(col('Start'), "M-d-yyyy")) \
    .withColumn("Finish", to_date(col('Finish'), "M-d-yyyy"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(schedule_revised)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schedule_revised.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/oh_ip/schedule_revised")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schedule Baseline

# CELL ********************

schema = StructType([
    StructField('Task_number', LongType(), True), 
    StructField('Outline_number', StringType(), True), 
    StructField('Name', StringType(), True), 
    StructField('Duration_days', StringType(), True),
    StructField('Start', StringType(), True),
    StructField('Finish', StringType(), True),
    StructField('Depends_on', StringType(), True),
    StructField('Dependents_after', StringType(), True),
    StructField('Percent_complete', DoubleType(), True),
    StructField('Notes', StringType(), True),
    StructField('Assigned_to', StringType(), True),
    StructField('Bucket', StringType(), True),
    StructField('Effort', StringType(), True),
    StructField('Effort_completed_hours', StringType(), True),
    StructField('Effort_remaining_hours', StringType(), True),
    StructField('Milestone', StringType(), True),
    StructField('Notes_1', StringType(), True),
    StructField('Completed', StringType(), True),
    StructField('Checklist_items', StringType(), True),
    StructField('Labels', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('Sprint', StringType(), True),
    StructField('Goal', StringType(), True),
])    

schedule_baseline = spark.read.format("csv") \
    .option("header","true") \
    .schema(schema) \
    .load(rootRead + "schedule_baseline")

schedule_baseline = schedule_baseline.withColumn('Source', regexp_extract(F.input_file_name(), ".*/([^/]+\\.csv)", 1)) \
    .withColumn('Source_Date', to_date(regexp_extract(col('Source'), r"(\d{4}_\d{2}_\d{2})", 1), 'yyyy_MM_dd')) \
    .withColumn("Start", to_date(col('Start'), "M-d-yyyy")) \
    .withColumn("Finish", to_date(col('Finish'), "M-d-yyyy"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(schedule_baseline)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schedule_baseline.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/oh_ip/schedule_baseline")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Financial Actuals

# CELL ********************

schema = StructType([
    StructField('RefDocNo', LongType(), True), 
    StructField('Val.type_text', StringType(), True), 
    StructField('CoCd', LongType(), True), 
    StructField('Year', LongType(), True),
    StructField('Prd', LongType(), True),
    StructField('Posting_Day', DateType(), True),
    StructField('Doc._Date', DateType(), True),
    StructField('Cost_Ctr', StringType(), True),
    StructField('Fund', StringType(), True),
    StructField('GL', StringType(), True),
    StructField('F.Ar', StringType(), True),
    StructField('FundedPrg', StringType(), True),
    StructField('Cmmt_item', LongType(), True),
    StructField('Customer', StringType(), True),
    StructField('Vendor', LongType(), True),
    StructField('Name', StringType(), True),
    StructField('Type', StringType(), True),
    StructField('Order', StringType(), True),
    StructField('Prd.doc.no', LongType(), True),
    StructField('Pred.DI', LongType(), True),
    StructField('Desc.', StringType(), True),
    StructField('Descr.', StringType(), True),
    StructField('Text', StringType(), True),
    StructField('PymntBdgt', DoubleType(), True),
])

financial_actuals = spark.read.format("csv") \
    .option("header","true") \
    .schema(schema) \
    .load(rootRead + "financial_actuals")

financial_actuals = financial_actuals.withColumn('Source', regexp_extract(F.input_file_name(), ".*/([^/]+\\.csv)", 1)) \
    .withColumn('Source_Date', to_date(regexp_extract(col('Source'), r"(\d{4}-\d{2}-\d{2})", 1), 'yyyy-MM-dd'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(financial_actuals)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

financial_actuals.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/oh_ip/financial_actuals")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Financial Estimates

# CELL ********************

schema = StructType([
    StructField('Fiscal_Year', StringType(), True), 
    StructField('Stages', StringType(), True), 
    StructField('Branch', StringType(), True), 
    StructField('Directorate', StringType(), True),
    StructField('Activity', StringType(), True),
    StructField('Description', StringType(), True),
    StructField('Branch_Payee', StringType(), True),
    StructField('Directorate_Payee_and_Comments', StringType(), True),
    StructField('Resources', StringType(), True),
    StructField('Unit_of_Efforts', StringType(), True),
    StructField('LOE', LongType(), True),
    StructField('Unit_cost', DoubleType(), True),
    StructField('Number_of_units', LongType(), True),
    StructField('Percentage_Applied', DoubleType(), True),
    StructField('Vote', StringType(), True),
    StructField('Contigency_percentage', DoubleType(), True),
    StructField('Salary_cost', LongType(), True),
    StructField('LOE_Cost', LongType(), True),
    StructField('Salary_LOE_Cost', LongType(), True),
    StructField('EBP', LongType(), True),
    StructField('Non_Salary', LongType(), True),
    StructField('Estimated_Overhead_Cost', LongType(), True),
    StructField('Total_Cost_excl_Contigency', LongType(), True),
    StructField('Contigency', LongType(), True),
    StructField('Total_Cost_incl_Contigency', DoubleType(), True),
    StructField('Total_Vote1_excl_Contigency', DoubleType(), True),
    StructField('Total_Vote5_excl_Contigency', DoubleType(), True),
    StructField('Total_Vote1and5_excl_Contigency', DoubleType(), True),
    StructField('Total_Vote1_incl_Contigency', DoubleType(), True),
    StructField('Total_Vote5_incl_Contigency', DoubleType(), True),
    StructField('Total_Vote1and5_incl_Contigency', DoubleType(), True),
    StructField('Ongoing_Salary', DoubleType(), True),
    StructField('Ongoing_EBP', DoubleType(), True),
    StructField('Ongoing_non_salary', DoubleType(), True),
    StructField('Total_Ongoing', LongType(), True),
    StructField('Funds', StringType(), True),
    StructField('Actuals', StringType(), True),
    StructField('Commitment', LongType(), True),
    StructField('EBP_Calc', LongType(), True),
    StructField('Total_Actuals', LongType(), True),
    StructField('Variance', LongType(), True),
    StructField('Notes_and_Comments', StringType(), True),
    StructField('List_of_GC_Docs', StringType(), True),
])

financial_estimates = spark.read.format("csv") \
    .option("header","true") \
    .option("multiline","true") \
    .option("escape","/") \
    .schema(schema) \
    .load(rootRead + "financial_estimates")

financial_estimates = financial_estimates.withColumn('Source', regexp_extract(F.input_file_name(), ".*/([^/]+\\.csv)", 1)) \
    .withColumn('Source_Date', to_date(regexp_extract(col('Source'), r"(\d{4}-\d{2}-\d{2})", 1), 'yyyy-MM-dd'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#display(financial_estimates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

financial_estimates.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/oh_ip/financial_estimates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
