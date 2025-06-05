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
# META       "default_lakehouse_workspace_id": "4748fbe5-9b18-4aac-9d74-f79c39ff81db"
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

cl90_full = spark.sql("SELECT * FROM Gold_Lakehouse_DEV.sunlife_gold.cl90").toPandas()
cl92_full = spark.sql("SELECT * FROM Gold_Lakehouse_DEV.sunlife_gold.cl92").toPandas()
codes = spark.sql("SELECT * FROM Gold_Lakehouse_DEV.hc.procedure_codes").toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cl90_submitted = cl90_full.query('Procedure_Code_Submitted.notna()').copy()
cl90_paid = cl90_full.query('Procedure_Code_Paid.notna()').copy()

cl90_submitted = cl90_submitted.merge(codes, how = 'left', left_on = 'Code_Join_Submitted', right_on = 'Code_Join')
cl90_paid = cl90_paid.merge(codes, how = 'left', left_on = 'Code_Join_Paid', right_on = 'Code_Join')

cl90_submitted_empty = cl90_submitted.query('Code_Join.isna()')
cl90_paid_empty = cl90_paid.query('Code_Join.isna()')

cl90_submitted_empty_unique = cl90_submitted_empty.drop_duplicates(subset = 'Code_Join_Submitted')
cl90_paid_empty_unique = cl90_paid_empty.drop_duplicates(subset = 'Code_Join_Paid')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(cl90_submitted_empty.query('Procedure_Code_Submitted == "00111"'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("CL90: " + str(len(cl90_full.index)))
print("Submitted Total: " + str(len(cl90_submitted.index)))
print("Paid Total: " + str(len(cl90_paid.index)))
print("Submitted No Join: " + str(len(cl90_submitted_empty.index)))
print("Paid No Join: " + str(len(cl90_paid_empty.index)))
print("Submitted No Join Unique: " + str(len(cl90_submitted_empty_unique.index)))
print("Paid No Join Unique: " + str(len(cl90_paid_empty_unique.index)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cl92_submitted = cl92_full.query('Procedure_Code_Submitted.notna()').copy()
cl92_paid = cl92_full.query('Procedure_Code_Paid.notna()').copy()

cl92_submitted = cl92_submitted.merge(codes, how = 'left', left_on = 'Code_Join_Submitted', right_on = 'Code_Join')
cl92_paid = cl92_paid.merge(codes, how = 'left', left_on = 'Code_Join_Paid', right_on = 'Code_Join')

cl92_submitted_empty = cl92_submitted.query('Code_Join.isna()')
cl92_paid_empty = cl92_paid.query('Code_Join.isna()')

cl92_submitted_empty_unique = cl92_submitted_empty.drop_duplicates(subset = 'Code_Join_Submitted')
cl92_paid_empty_unique = cl92_paid_empty.drop_duplicates(subset = 'Code_Join_Paid')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("CL92: " + str(len(cl92_full.index)))
print("Submitted Total: " + str(len(cl92_submitted.index)))
print("Paid Total: " + str(len(cl92_paid.index)))
print("Submitted No Join: " + str(len(cl92_submitted_empty.index)))
print("Paid No Join: " + str(len(cl92_paid_empty.index)))
print("Submitted No Join Unique: " + str(len(cl92_submitted_empty_unique.index)))
print("Paid No Join Unique: " + str(len(cl92_paid_empty_unique.index)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

keepColsSubmitted = [
    'Procedure_Code_Submitted',
    'SL_Procedure_Submitted_Description_EN',
    'Benefit_Category',
    'Tooth_Number',
    'Tooth_Surface',
    'Schedule_x',
    'Facility_PT',
    'Specialty_x'
]

renameColsSubmitted = {
    'Procedure_Code_Submitted': 'Procedure_Code',
    'SL_Procedure_Submitted_Description_EN': 'Sunlife_Code_Description',
    'Schedule_x': 'Sunlife_Schedule',
    'Specialty_x': 'Specialty',
}

keepColsPaid= [
    'Procedure_Code_Paid',
    'SL_Procedure_Paid_Description_EN',
    'Benefit_Category',
    'Tooth_Number',
    'Tooth_Surface',
    'Schedule_x',
    'Facility_PT',
    'Specialty_x'
]

renameColsPaid = {
    'Procedure_Code_Paid': 'Procedure_Code',
    'SL_Procedure_Paid_Description_EN': 'Sunlife_Code_Description',
    'Schedule_x': 'Sunlife_Schedule',
    'Specialty_x': 'Specialty',
}

cl90_submitted_empty_unique = cl90_submitted_empty_unique[keepColsSubmitted].rename(columns = renameColsSubmitted)
cl90_paid_empty_unique = cl90_paid_empty_unique[keepColsPaid].rename(columns = renameColsPaid)
cl92_submitted_empty_unique = cl92_submitted_empty_unique[keepColsSubmitted].rename(columns = renameColsSubmitted)
cl92_paid_empty_unique = cl92_paid_empty_unique[keepColsPaid].rename(columns = renameColsPaid)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

appended_unique_codes = pd.concat([cl90_submitted_empty_unique, cl90_paid_empty_unique, cl92_submitted_empty_unique, cl92_paid_empty_unique])
actual_unique_codes = appended_unique_codes \
    .drop_duplicates(subset = 'Procedure_Code') \
    .sort_values(by = ['Procedure_Code'])

if not actual_unique_codes.index.is_unique:
    actual_unique_codes.reset_index(drop=True, inplace=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Total Unique: " + str(len(appended_unique_codes.index)))
print("Total Actual Unique: " + str(len(actual_unique_codes)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

first_set = actual_unique_codes[actual_unique_codes['Procedure_Code'].str.match(r'^\d')]
second_set = actual_unique_codes[~actual_unique_codes['Procedure_Code'].str.match(r'^\d')]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
# Testing Procedure Codes - fixing Code_Join
df_transformed['Test'] = np.where(
    pd.isnull(df_transformed['Procedure_Code_Submitted']), 
    None,  # Assign None where procedureCodeCol is null
    np.where(
        df_transformed['Specialty'].isin(['DT', 'HY']),
        df_transformed['Procedure_Code_Submitted'] + '-' + df_transformed['Specialty'],
        np.where(
            (df_transformed['QC_Flag'] == 'QC') | (df_transformed['Specialty'] == "OS"),
            df_transformed['Procedure_Code_Submitted'] + '-' + df_transformed['QC_Flag'] + '-' + df_transformed['Specialty'],
            df_transformed['Procedure_Code_Submitted'] + '-' + df_transformed['QC_Flag']
        )
    )
)"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(first_set)
display(second_set)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Display

# CELL ********************



display(actual_unique_codes.query("Procedure_Code.str.match(r'^\d')]"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(cl90_submitted_empty_unique)
display(cl90_paid_empty_unique)
display(cl92_submitted_empty_unique)
display(cl92_paid_empty_unique)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
