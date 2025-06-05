# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Preamble

# MARKDOWN ********************

# # Schemas

# MARKDOWN ********************

# ## CL90 Schema

# CELL ********************

readSchemaCL90 = {
    'H': str,
    'Claim Reference Number' : str,
    'Member ID' : str,
    'Member Province' : str,
    'Member Postal Code': str,
    'Age': 'Int64',
    'Provider ID': str,
    'Provider Facility ID': str,
    'Provider Province': str,
    'Provider Postal Code': str,
    'Provider Participation Type': str,
    'Specialty': str,
    'Co-Pay(Plan ID)': str,
    'Benefit Category': str,
    'Procedure Code': str,
    'Procedure Code Paid': str,
    'Tooth Number': str,
    'Tooth Surface': str,
    'Post-Determination Indicator': str,
    'Schedule': str,
    'Service Date': str, # date
    'Submitted Date': str, # date
    'Adjudicated Date': str, # date
    'Amended Date': str, # date
    'Paper Claim Indicator': str,
    'Reason Code': str,
    'Remark Code': str,
    'Cheque Number': str,
    'Payment Method': str,
    'Coordination of Benefits Indicator': str,
    'Submitted Amount': float,
    'Eligible Amount': float,
    'Coordination of Benefits Amount': float,
    'Paid Amount': float,
    'Analysis Date ': str, # date
}

dateColsCL90 = [
    'Service Date',
    'Submitted Date',
    'Adjudicated Date',
    'Amended Date',
    'Analysis Date '
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaCL90 = StructType([  
    StructField('Claim_Reference_Number', StringType(), True),
    StructField('Member_ID', StringType(), True), 
    StructField('Member_PT', StringType(), True),
    StructField('Member_PT_Alt', StringType(), True),
    StructField('Member_Postal_Code', StringType(), True),
    StructField('Member_FSA', StringType(), True),
    StructField('Age', IntegerType(), True),
    StructField('Provider_ID', StringType(), True),
    StructField('Facility_ID', StringType(), True),
    StructField('QC_Flag', StringType(), True),
    StructField('Facility_PT', StringType(), True),
    StructField('Facility_PT_Alt', StringType(), True),
    StructField('Facility_Postal_Code', StringType(), True),
    StructField('Facility_FSA', StringType(), True),
    StructField('Provider_Participation_Type', StringType(), True),
    StructField('Specialty', StringType(), True),
    StructField('Co_Pay_Tier', StringType(), True),
    StructField('Benefit_Category', StringType(), True),
    StructField('Procedure_Code_Submitted', StringType(), True),
    StructField('Code_Join_Submitted', StringType(), True),
    StructField('SL_Procedure_Submitted_Description_EN', StringType(), True),
    StructField('SL_Procedure_Submitted_Description_FR', StringType(), True),
    StructField('Procedure_Code_Paid', StringType(), True),
    StructField('Code_Join_Paid', StringType(), True),
    StructField('SL_Procedure_Paid_Description_EN', StringType(), True),
    StructField('SL_Procedure_Paid_Description_FR', StringType(), True),
    StructField('Tooth_Number', StringType(), True),
    StructField('Tooth_Surface', StringType(), True),
    StructField('Post_Determination_Indicator', StringType(), True),
    StructField('Schedule', StringType(), True),
    StructField('Service_Date', DateType(), True),
    StructField('Claim_Date', DateType(), True),
    StructField('Submitted_Date', DateType(), True),
    StructField('Adjudicated_Date', DateType(), True),
    StructField('Amended_Date', DateType(), True),
    StructField('Analysis_Date', DateType(), True),
    StructField('Paper_Claim_Indicator', StringType(), True),
    StructField('Reason_Code', StringType(), True),
    StructField('Reason_Desription', StringType(), True),
    StructField('Remark_Code', StringType(), True),
    StructField('Remark_Description', StringType(), True),
    StructField('Cheque_Number', StringType(), True),
    StructField('Payment_Method', StringType(), True),
    StructField('COB_Indicator', BooleanType(), True),
    StructField('Submitted_Amount', DoubleType(), True),
    StructField('Eligible_Amount', DoubleType(), True),
    StructField('COB_Amount', DoubleType(), True),
    StructField('Paid_Amount', DoubleType(), True),
    StructField('Source', DateType(), True)
]) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## CL92 Schema

# CELL ********************

readSchemaCL92 = {
    'H': str,
    'Claim Reference Number' : str,
    'Member ID' : str,
    'Member Province' : str,
    'Member Postal Code': str,
    'Age': 'Int64',
    'Provider ID': str,
    'Provider Facility ID': str,
    'Provider Province': str,
    'Provider Postal Code': str,
    'Provider Participation Type': str,
    'Specialty': str,
    'Co-Pay(Plan ID)': str,
    'Benefit Category': str,
    'Procedure Code': str,
    'Procedure Code Paid': str,
    'Tooth Number': str,
    'Tooth Surface': str,
    'Preauthorization Indicator': str,
    'Schedule': str,
    'Service Date': str, # date
    'Submitted Date': str, # date
    'Adjudicated Date': str, # date
    'Paper Claim Indicator': str,
    'Reason Code': str,
    'Remark Code': str,
    'Submitted Amount': float,
    'Eligible Amount': float,
    'Paid Amount': float,
    'Analysis Date ': str # date
}

dateColsCL92 = [
    'Service Date',
    'Submitted Date',
    'Adjudicated Date',
    'Analysis Date '
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaCL92 = StructType([  
    StructField('Claim_Reference_Number', StringType(), True),
    StructField('Member_ID', StringType(), True), 
    StructField('Member_PT', StringType(), True),
    StructField('Member_PT_Alt', StringType(), True),
    StructField('Member_Postal_Code', StringType(), True),
    StructField('Member_FSA', StringType(), True),
    StructField('Age', IntegerType(), True),
    StructField('Provider_ID', StringType(), True),
    StructField('Facility_ID', StringType(), True),
    StructField('QC_Flag', StringType(), True),
    StructField('Facility_PT', StringType(), True),
    StructField('Facility_PT_Alt', StringType(), True),
    StructField('Facility_Postal_Code', StringType(), True),
    StructField('Facility_FSA', StringType(), True),
    StructField('Provider_Participation_Type', StringType(), True),
    StructField('Specialty', StringType(), True),
    StructField('Co_Pay_Tier', StringType(), True),
    StructField('Benefit_Category', StringType(), True),
    StructField('Procedure_Code_Submitted', StringType(), True),
    StructField('Code_Join_Submitted', StringType(), True),
    StructField('SL_Procedure_Submitted_Description_EN', StringType(), True),
    StructField('SL_Procedure_Submitted_Description_FR', StringType(), True),
    StructField('Procedure_Code_Paid', StringType(), True),
    StructField('Code_Join_Paid', StringType(), True),
    StructField('SL_Procedure_Paid_Description_EN', StringType(), True),
    StructField('SL_Procedure_Paid_Description_FR', StringType(), True),
    StructField('Tooth_Number', StringType(), True),
    StructField('Tooth_Surface', StringType(), True),
    StructField('Preauthorization_Indicator', StringType(), True),
    StructField('Schedule', StringType(), True),
    StructField('Service_Date', DateType(), True),
    StructField('Claim_Date', DateType(), True),
    StructField('Submitted_Date', DateType(), True),
    StructField('Adjudicated_Date', DateType(), True),
    StructField('Analysis_Date', DateType(), True),
    StructField('Paper_Claim_Indicator', StringType(), True),
    StructField('Reason_Code', StringType(), True),
    StructField('Reason_Desription', StringType(), True),
    StructField('Remark_Code', StringType(), True),
    StructField('Remark_Description', StringType(), True),
    StructField('Submitted_Amount', DoubleType(), True),
    StructField('Eligible_Amount', DoubleType(), True),
    StructField('Paid_Amount', DoubleType(), True),
    StructField('Source', DateType(), True)
]) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FI02 Schema

# CELL ********************

readSchemaFI02 = {
    'Provider Number': str,
    'Provider Facility ID': str,
    'Member ID': str,
    'Claim Reference Number': str,
    'Cheque Number': str,
    'Billing Group': str,
    'Member Province/Territories': str,
    'Member Postal Code': str,
    'Provider Province/Territories': str,
    'Provider Postal Code': str,
    'CFR Date': str, # date
    'Adjudicated Date': str, # date
    'EFT': float,
    'Cheques': float,
    'Payment Cancellations EFT': float,
    'Payment Cancellations Cheque': float,
    'Refunds EFT': float,
    'Refunds Cheque': float,
    'Other Adjustments': float,
    'Voids EFT': float,
    'Voids Cheque': float
}

dateColsFI02 = [
    'CFR Date',
    'Adjudicated Date'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaFI02 = StructType([ 
    StructField('Provider_ID', StringType(), True), 
    StructField('Facility_ID', StringType(), True), 
    StructField('Member_ID', StringType(), True), 
    StructField('Claim_Reference_Number', StringType(), True),
    StructField('Cheque_Number', StringType(), True), 
    StructField('Billing_Group', StringType(), True),
    StructField('Member_PT', StringType(), True),
    StructField('Member_PT_Alt', StringType(), True),
    StructField('Member_Postal_Code', StringType(), True),
    StructField('Member_FSA', StringType(), True),
    StructField('Facility_PT', StringType(), True),
    StructField('Facility_PT_Alt', StringType(), True),
    StructField('Facility_Postal_Code', StringType(), True),
    StructField('Facility_FSA', StringType(), True),
    StructField('CFR_Date', DateType(), True),
    StructField('Claim_Date', DateType(), True),
    StructField('Adjudicated_Date', DateType(), True),
    StructField('EFT', DoubleType(), True),
    StructField('Cheques', DoubleType(), True),
    StructField('Payment_Cancellation_EFT', DoubleType(), True),
    StructField('Payment_Cancellation_Cheque', DoubleType(), True),
    StructField('Refunds_EFT', DoubleType(), True),
    StructField('Refunds_Cheque', DoubleType(), True),
    StructField('Other_Adjustments', DoubleType(), True),
    StructField('Voids_EFT', DoubleType(), True),
    StructField('Voids_Cheque', DoubleType(), True),
    StructField('Total_Amount', DoubleType(), True),
    StructField('Source', DateType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PP08 Schema

# CELL ********************

readSchemaPP08 = {
    'Provider ID': str, 
    'PT': str, 
    'Provider Postal Code': str, 
    'Participating Date': str, # date 
    'Specialty': str,
    'Language': str,
    'Direct Billing': str
}

dateColsPP08 = [
    'Participating Date'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaPP08 = StructType([ 
    StructField('Provider_ID', StringType(), False), 
    StructField('Provider_PT', StringType(), True), 
    StructField('Provider_PT_Alt', StringType(), True), 
    StructField('Provider_Postal_Code', StringType(), True), 
    StructField('Provider_FSA', StringType(), True), 
    StructField('Provider_Area_Type', StringType(), True), 
    StructField('Participating_Date', DateType(), True), 
    StructField('Enrolled_Status', BooleanType(), True), 
    StructField('Specialty', StringType(), True),
    StructField('Language', StringType(), True),
    StructField('Direct_Billing', BooleanType(), True),
    StructField('Source', DateType(), True)
])  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Contact Center Metrics Schema

# CELL ********************

readSchemaContactCenterMetrics = {
    'Queue_Category': str, 
    'Language': str, 
    'Contact_Date': str, #date
    'Service_Level': float,
    'Average_Handle_Time': float,
    'Average_Wait_Time': float,
    'Contacts_Abandoned': float,
    'Contacts_Handled_Incoming': float,
    'Callback Contacts Handled': float,
    'Total_Calls': float
}

dateColsContactCenterMetrics = [
    'Contact_Date'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaContactCenterMetrics = StructType([
    StructField('Queue_Category', StringType(), True), 
    StructField('Language', StringType(), True), 
    StructField('Language_FR', StringType(), True), 
    StructField('Contact_Date', DateType(), True), 
    StructField('Service_Level', DoubleType(), True),
    StructField('Average_Handle_Time', LongType(), True),
    StructField('Average_Handle_Time_Mins', DoubleType(), True),
    StructField('Average_Wait_Time', LongType(), True),
    StructField('Contacts_Abandoned', LongType(), True),
    StructField('Contacts_Handled_Incoming', LongType(), True),
    StructField('Callback_Contacts_Handled', LongType(), True),
    StructField('Total_Calls', LongType(), True),
    StructField('Source', DateType(), True)
])  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ESDC Applications PT Schema

# CELL ********************

readSchemaApplicationsPT = {
    'Month of Submission': str, #date
    'Applicant Type': str, 
    'Province / Territory': str,
    'Urban / Rural': str,
    'Age Range': str,
    'Language': str,
    'Application Received': str,
    'Application Completed': str,
    'Eligibility': str,
    'Applicant-Stated Disability': str,
    'CRA-Validated Disability': str,
    'Renewal': str,
    'Total Count': float
}

dateColsApplicationsPT = [
    'Month of Submission'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaApplicationsPT = StructType([
    StructField("Submission_Month", DateType(), True),
    StructField("Applicant_Type", StringType(), True),
    StructField("Member_PT", StringType(), True),
    StructField("Member_Area_Type", StringType(), True),
    StructField("Member_Area_Type_FR", StringType(), True),
    StructField("Age_Range", StringType(), True),
    StructField("Language", StringType(), True),
    StructField('Language_FR', StringType(), True), 
    StructField("Application_Received", StringType(), True),
    StructField("Application_Completed", StringType(), True),
    StructField("Eligibility", StringType(), True),
    StructField("Disability", StringType(), True),
    StructField("Renewal", StringType(), True),
    StructField("Total_Count", LongType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ESDC Enrolled PT

# CELL ********************

readSchemaEnrolledPT = {
    'Month of Enrolment': str, #date
    'Province / Territory': str,
    'Urban / Rural': str,
    'Age Range': str,
    'Language': str,
    'Total Count': float
}

dateColsEnrolledPT = [
    'Month of Enrolment'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaEnrolledPT = StructType([
    StructField("Enrolment_Month", DateType(), True),
    StructField("Member_PT", StringType(), True),
    StructField("Member_Area_Type", StringType(), True),
    StructField("Member_Area_Type_FR", StringType(), True),
    StructField("Age_Range", StringType(), True),
    StructField("Language", StringType(), True),
    StructField("Language_FR", StringType(), True),
    StructField("Total_Count", LongType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ESDC Applications FSA

# CELL ********************

readSchemaApplicationsFSA = {
    'Month of Submission': str, #date
    'FSA': str,
    'Province / Territory': str,
    'Age Range': str,
    'Renewal': str,
    'Total Eligible Applicants': float
}

dateColsApplicationsFSA = [
    'Month of Submission'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaApplicationsFSA = StructType([
    StructField("Submission_Month", DateType(), True),
    StructField("FSA", StringType(), True),
    StructField("Member_PT", StringType(), True),
    StructField("Age_Range", StringType(), True),
    StructField("Renewal", StringType(), True),
    StructField("Total", LongType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ESDC Enrolled FSA

# CELL ********************

readSchemaEnrolledFSA = {
    'Month of Enrolment': str, #date
    'FSA': str,
    'Province / Territory': str,
    'Age Range': str,
    'Total Enroled Clients': float
}

dateColsEnrolledFSA = [
    'Month of Enrolment'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaEnrolledFSA = StructType([
    StructField("Enrollment_Month", DateType(), True),
    StructField("FSA", StringType(), True),
    StructField("Member_PT", StringType(), True),
    StructField("Age_Range", StringType(), True),
    StructField("Total", LongType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Procedure Codes Schema

# CELL ********************

readSchemaProcedureCodes = {
    'Category_EN': str,
    'Category_FR': str,
    'Class_EN': str,
    'Class_FR': str,
    'Sub_Class_EN': str,
    'Sub_Class_FR': str,
    'Service_Title_EN': str,
    'Service_Title_FR': str,
    'Description_EN': str,
    'Description_FR': str,
    'Code': str,
    'QC Code': str,
    'Association': str,
    'A': str,
    'B': str,
    'Z': str,
    'C': str,
    'D': str,
    'Notes': str,
    'Additional Notes': str
}

dateColsProcedureCodes = [
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaProcedureCodes = StructType([
    StructField("ID", StringType(), True),
    StructField("Code", StringType(), True),
    StructField("Code_Base", StringType(), True),
    StructField("Code_Join", StringType(), True),
    StructField("QC_Flag", StringType(), True),
    StructField("Association", StringType(), True),
    StructField("Specialty", StringType(), True),
    StructField("Schedule", StringType(), True),
    StructField("Category_EN", StringType(), True),
    StructField("Category_FR", StringType(), True),
    StructField("Class_EN", StringType(), True),
    StructField("Class_FR", StringType(), True),
    StructField("Sub_Class_EN", StringType(), True),
    StructField("Sub_Class_FR", StringType(), True),
    StructField("Service_Title_EN", StringType(), True),
    StructField("Service_Title_FR", StringType(), True),
    StructField("Description_EN", StringType(), True),
    StructField("Description_FR", StringType(), True),
    StructField("Notes", StringType(), True),
    StructField("Additional_Notes", StringType(), True),
    StructField("Exception_Start_Date", DateType(), True),
    StructField("Start_Date", DateType(), True),
    StructField("End_Date", DateType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Eligible Population Estimates

# CELL ********************

readSchemaEligiblePopulationEstimates = {
    'PT': str,
    'U18': 'Int64',
    'PWD': 'Int64',
    '65 to 69': 'Int64',
    '70 to 71': 'Int64',
    '72 to 76': 'Int64',
    '77 to 86': 'Int64',
    '87+': 'Int64'
}

dateColsEligiblePopulationEstimates = [
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaEligiblePopulationEstimates = StructType([
    StructField("Population", DoubleType(), True),
    StructField("PT", StringType(), True),
    StructField("Age_Group", StringType(), True),
    StructField("Latest_Age", IntegerType(), True),
    StructField("Source", DateType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Reason and Remark Codes

# CELL ********************

readSchemaReasonAndRemarkCodes = {
    'Reason_Code': str,
    'Reason_Description': str,
}

dateColsReasonAndRemarkCodes = [
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaReasonAndRemarkCodes = StructType([
    StructField("Reason_Code", StringType(), True),
    StructField("Reason_Description", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Procedure Code Descriptions

# CELL ********************

readSchemaProcedureCodeDescriptions = {
    'SL_Procedure_Code': str,
    'SL_Procedure_Description_EN': str,
    'SL_Procedure_Description_EN': str
}

dateColsProcedureCodeDescriptions = [
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaProcedureCodeDescriptions = StructType([
    StructField("SL_Procedure_Code", StringType(), True),
    StructField("SL_Procedure_Description_EN", StringType(), True),
    StructField("SL_Procedure_Description_FR", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sunlife Costing

# CELL ********************

readSchemaSunlifeCosting = {
    'Month' : str, #date
    'Contract_Budget_FY' : float,
    'Contract_Expenditure' : float,
    'Benefit_Budget_FY' : float,
    'Benefit_Expenditure' : float
}

dateColsSunlifeCosting = [
    'Month'
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaSunlifeCosting = StructType ([
    StructField('FY', StringType(), True),
    StructField('Month', DateType(), True),
    StructField('Contract_Budget_FY', DoubleType(), True),
    StructField('Contract_Expenditure', DoubleType(), True),
    StructField('Contract_Expenditure_FY', DoubleType(), True),
    StructField('Contract_Budget_Remaining_FY', DoubleType(), True),
    StructField('Benefit_Budget_FY', DoubleType(), True),
    StructField('Benefit_Expenditure', DoubleType(), True),
    StructField('Benefit_Expenditure_FY', DoubleType(), True),
    StructField('Benefit_Budget_Remaining_FY', DoubleType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PCCF

# CELL ********************

readSchemaPCCF = {
    'PostalCode' : str,
    'FSA' : str,
    'PR' : str,
    'CDUid' : str,
    'CSDUid' : str,
    'CSDName' : str,
    'CSDType' : str,
    'CCSCode' : str,
    'SAC' : str,
    'SACType' : str,
    'CTName' : str,
    'ER' : str,
    'DPL' : str,
    'FED13uid' : str,
    'POP_CNTR_RA' : str,
    'POP_CNTR_RA_type' : str,
    'DAuid' : str,
    'DisseminationBlock' : str,
    'Rep_Pt_Type' : str,
    'SLI' : str,
    'PCtype' : str,
    'Comm_Name' : str,
    'DMT' : str,
    'H_DMT' : str,
    'Birth_Date' : str,
    'Ret_Date' : str,
    'PO' : str,
    'QI' : str,
    'Source' : str,
    'POP_CNTR_RA_SIZE_CLASS' : str,
    'LAT' : float,
    'LONG' : float
}

dateColsPCCF = [
    
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaPCCF = StructType ([
    StructField('Postal_Code', StringType(), True),
    StructField('FSA', StringType(), True),
    StructField('Area_Type', StringType(), True),
    StructField('PT', StringType(), True),
    StructField('PT_Long', StringType(), True),
    StructField('PT_Long_Sort', IntegerType(), True),
    StructField('PT_Group_1', StringType(), True),
    StructField('PT_Group_1_FR', StringType(), True),
    StructField('PT_Group_1_Sort', IntegerType(), True),
    StructField('PT_Group_2', StringType(), True),
    StructField('PT_Group_2_FR', StringType(), True),
    StructField('PT_Group_2_Sort', IntegerType(), True),
    StructField('PT_Group_3', StringType(), True),
    StructField('PT_Group_3_FR', StringType(), True),
    StructField('PT_Group_3_Sort', IntegerType(), True),
    StructField('CSD_ID', StringType(), True),
    StructField('CSD_Name', StringType(), True),
    StructField('Community_Name', StringType(), True),
    StructField('Latitude', DoubleType(), True),
    StructField('Longitude', DoubleType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ESDC Member Schema

# CELL ********************

providerDetailsSchema = StructType([
    StructField("esdc_iscoveredraw", StringType(), True),
    StructField("esdc_providertype", StringType(), True),
    StructField("esdc_providertype_en", StringType(), True),
    StructField("esdc_providertype_fr", StringType(), True),
    StructField("esdc_employername", StringType(), True),
    StructField("esdc_employeraddress", StringType(), True),
    StructField("esdc_employerphonenumber", StringType(), True)
])

spouseProviderDetailsSchema = StructType([
    StructField("spouse_esdc_iscoveredraw", StringType(), True),
    StructField("spouse_esdc_providertype", StringType(), True),
    StructField("spouse_esdc_providertype_en", StringType(), True),
    StructField("spouse_esdc_providertype_fr", StringType(), True),
    StructField("spouse_esdc_employername", StringType(), True),
    StructField("spouse_esdc_employeraddress", StringType(), True),
    StructField("spouse_esdc_employerphonenumber", StringType(), True)
])

readSchemaESDCMembers = StructType([  
    StructField('dfp_incremental_id', StringType(), True),
    StructField('dfp_change', StringType(), True),
    StructField('uniqueid', StringType(), True), 
    StructField('esdc_dateofapplication', DateType(), True),
    StructField('esdc_applicanttype', StringType(), True),
    StructField('esdc_applicanttype_en', StringType(), True),
    StructField('esdc_applicanttype_fr', StringType(), True),
    StructField('esdc_basetaxyear', StringType(), True),
    StructField('esdc_familyid', StringType(), True),
    StructField('esdc_overagedependent', StringType(), True),
    StructField('esdc_overagedependent_en', StringType(), True),
    StructField('esdc_overagedependent_fr', StringType(), True),
    StructField('esdc_clientnumber', StringType(), True),
    StructField('esdc_firstname', StringType(), True),
    StructField('esdc_lastname', StringType(), True),
    StructField('esdc_socialinsurancenumber', StringType(), True),
    StructField('esdc_dateofbirth', DateType(), True),
    StructField('esdc_sex', StringType(), True),
    StructField('esdc_sex_en', StringType(), True),
    StructField('esdc_sex_fr', StringType(), True),
    StructField('emailaddress', StringType(), True),
    StructField('esdc_phonenumber', StringType(), True),
    StructField('esdc_preferredlanguage', StringType(), True),
    StructField('esdc_preferredlanguage_en', StringType(), True),
    StructField('esdc_preferredlanguage_fr', StringType(), True),
    StructField('esdc_preferredmethodofcommunication', StringType(), True),
    StructField('esdc_preferredmethodofcommunication_en', StringType(), True),
    StructField('esdc_preferredmethodofcommunication_fr', StringType(), True),
    StructField('esdc_maritalstatus', StringType(), True),
    StructField('esdc_maritalstatus_en', StringType(), True),
    StructField('esdc_maritalstatus_fr', StringType(), True),
    StructField('spouse_esdc_clientnumber', StringType(), True),
    StructField('spouse_esdc_socialinsurancenumber', StringType(), True),
    StructField('spouse_esdc_firstname', StringType(), True),
    StructField('spouse_esdc_lastname', StringType(), True),
    StructField('spouse_esdc_dateofbirth', DateType(), True),
    StructField('esdc_homeaddressstreet', StringType(), True),
    StructField('esdc_homeaddressapartmentunitnumber', StringType(), True),
    StructField('esdc_homeaddresscity', StringType(), True),
    StructField('esdc_homeaddresspostalzipcode', StringType(), True),
    StructField('esdc_homeaddressprovinceterritorystateid', StringType(), True),
    StructField('esdc_homeaddressprovinceterritorystate_en', StringType(), True),
    StructField('esdc_homeaddressprovinceterritorystate_fr', StringType(), True),
    StructField('esdc_homeaddresscountryid', StringType(), True),
    StructField('esdc_homeaddresscountry_en', StringType(), True),
    StructField('esdc_homeaddresscountry_fr', StringType(), True),
    StructField('esdc_mailingaddressstreet', StringType(), True),
    StructField('esdc_mailingaddressapartmentunitnumber', StringType(), True), 
    StructField('esdc_mailingaddresscity', StringType(), True),
    StructField('esdc_mailingaddresspostalzipcode', StringType(), True),
    StructField('esdc_mailingaddressprovinceterritorystateid', StringType(), True),
    StructField('esdc_mailingaddressprovinceterritorystate_en', StringType(), True),
    StructField('esdc_mailingaddressprovinceterritorystate_fr', StringType(), True),
    StructField('esdc_mailingaddresscountryid', StringType(), True),
    StructField('esdc_mailingaddresscountry_en', StringType(), True),
    StructField('esdc_mailingaddresscountry_fr', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageprovincialfederal', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageprovincialfederal_en', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageprovincialfederal_fr', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageemployerprivate', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageemployerprivate_en', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageemployerprivate_fr', StringType(), True),
    StructField('provincial_plan_esdc_nameid', StringType(), True),
    StructField('provincial_plan_esdc_name_en', StringType(), True),
    StructField('provincial_plan_esdc_name_fr', StringType(), True),
    StructField('federalprogram_escd_nameid', StringType(), True),
    StructField('federalprogram_escd_name_en', StringType(), True),
    StructField('federalprogram_escd_name_fr', StringType(), True),
    StructField('esdc_attestationcompletedon', DateType(), True),
    StructField('esdc_canadianresident', StringType(), True),
    StructField('esdc_canadianresident_en', StringType(), True),
    StructField('esdc_canadianresident_fr', StringType(), True),
    StructField('esdc_sharedcustodyname', StringType(), True),
    StructField('esdc_sharedcustodyname_en', StringType(), True),
    StructField('esdc_sharedcustodyname_fr', StringType(), True),
    StructField('esdc_disabilitytaxcredit', StringType(), True),
    StructField('esdc_disabilitytaxcredit_en', StringType(), True),
    StructField('esdc_disabilitytaxcredit_fr', StringType(), True),
    StructField('esdc_copaytier', StringType(), True),
    StructField('esdc_copaytier_en', StringType(), True),
    StructField('esdc_copaytier_fr', StringType(), True),
    StructField('esdc_provider_details', ArrayType(providerDetailsSchema), True),
    StructField('spouse_esdc_canadianresident', StringType(), True),
    StructField('spouse_esdc_canadianresident_en', StringType(), True),
    StructField('spouse_esdc_canadianresident_fr', StringType(), True),
    StructField('spouse_esdc_provider_details', ArrayType(spouseProviderDetailsSchema), True),
    StructField('esdc_eligibleonfirst', DateType(), True),
    StructField('esdc_enroledon', DateType(), True),
    StructField('esdc_batchdate', DateType(), True),
    StructField('esdc_coveragestart', DateType(), True),
    StructField('esdc_coverageend', DateType(), True),
    StructField('esdc_billinggroup', StringType(), True)
]) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaESDCMembers = StructType([  
    StructField('dfp_incremental_id', StringType(), True),
    StructField('dfp_change', StringType(), True),
    StructField('uniqueid', StringType(), True), 
    StructField('esdc_dateofapplication', DateType(), True),
    StructField('esdc_applicanttype', StringType(), True),
    StructField('esdc_applicanttype_en', StringType(), True),
    StructField('esdc_applicanttype_fr', StringType(), True),
    StructField('esdc_basetaxyear', StringType(), True),
    StructField('esdc_familyid', StringType(), True),
    StructField('esdc_overagedependent', StringType(), True),
    StructField('esdc_overagedependent_en', StringType(), True),
    StructField('esdc_overagedependent_fr', StringType(), True),
    StructField('esdc_clientnumber', StringType(), True),
    StructField('esdc_firstname', StringType(), True),
    StructField('esdc_lastname', StringType(), True),
    StructField('esdc_socialinsurancenumber', StringType(), True),
    StructField('esdc_dateofbirth', DateType(), True),
    StructField('esdc_sex', StringType(), True),
    StructField('esdc_sex_en', StringType(), True),
    StructField('esdc_sex_fr', StringType(), True),
    StructField('emailaddress', StringType(), True),
    StructField('esdc_phonenumber', StringType(), True),
    StructField('esdc_preferredlanguage', StringType(), True),
    StructField('esdc_preferredlanguage_en', StringType(), True),
    StructField('esdc_preferredlanguage_fr', StringType(), True),
    StructField('esdc_preferredmethodofcommunication', StringType(), True),
    StructField('esdc_preferredmethodofcommunication_en', StringType(), True),
    StructField('esdc_preferredmethodofcommunication_fr', StringType(), True),
    StructField('esdc_maritalstatus', StringType(), True),
    StructField('esdc_maritalstatus_en', StringType(), True),
    StructField('esdc_maritalstatus_fr', StringType(), True),
    StructField('spouse_esdc_clientnumber', StringType(), True),
    StructField('spouse_esdc_socialinsurancenumber', StringType(), True),
    StructField('spouse_esdc_firstname', StringType(), True),
    StructField('spouse_esdc_lastname', StringType(), True),
    StructField('spouse_esdc_dateofbirth', DateType(), True),
    StructField('esdc_homeaddressstreet', StringType(), True),
    StructField('esdc_homeaddressapartmentunitnumber', StringType(), True),
    StructField('esdc_homeaddresscity', StringType(), True),
    StructField('esdc_homeaddresspostalzipcode', StringType(), True),
    StructField('esdc_homeaddressprovinceterritorystateid', StringType(), True),
    StructField('esdc_homeaddressprovinceterritorystate_en', StringType(), True),
    StructField('esdc_homeaddressprovinceterritorystate_fr', StringType(), True),
    StructField('esdc_homeaddresscountryid', StringType(), True),
    StructField('esdc_homeaddresscountry_en', StringType(), True),
    StructField('esdc_homeaddresscountry_fr', StringType(), True),
    StructField('esdc_mailingaddressstreet', StringType(), True),
    StructField('esdc_mailingaddressapartmentunitnumber', StringType(), True), 
    StructField('esdc_mailingaddresscity', StringType(), True),
    StructField('esdc_mailingaddresspostalzipcode', StringType(), True),
    StructField('esdc_mailingaddressprovinceterritorystateid', StringType(), True),
    StructField('esdc_mailingaddressprovinceterritorystate_en', StringType(), True),
    StructField('esdc_mailingaddressprovinceterritorystate_fr', StringType(), True),
    StructField('esdc_mailingaddresscountryid', StringType(), True),
    StructField('esdc_mailingaddresscountry_en', StringType(), True),
    StructField('esdc_mailingaddresscountry_fr', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageprovincialfederal', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageprovincialfederal_en', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageprovincialfederal_fr', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageemployerprivate', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageemployerprivate_en', StringType(), True),
    StructField('esdc_hasdentalinsurancecoverageemployerprivate_fr', StringType(), True),
    StructField('provincial_plan_esdc_nameid', StringType(), True),
    StructField('provincial_plan_esdc_name_en', StringType(), True),
    StructField('provincial_plan_esdc_name_fr', StringType(), True),
    StructField('federalprogram_escd_nameid', StringType(), True),
    StructField('federalprogram_escd_name_en', StringType(), True),
    StructField('federalprogram_escd_name_fr', StringType(), True),
    StructField('esdc_attestationcompletedon', DateType(), True),
    StructField('esdc_canadianresident', StringType(), True),
    StructField('esdc_canadianresident_en', StringType(), True),
    StructField('esdc_canadianresident_fr', StringType(), True),
    StructField('esdc_sharedcustodyname', StringType(), True),
    StructField('esdc_sharedcustodyname_en', StringType(), True),
    StructField('esdc_sharedcustodyname_fr', StringType(), True),
    StructField('esdc_disabilitytaxcredit', StringType(), True),
    StructField('esdc_disabilitytaxcredit_en', StringType(), True),
    StructField('esdc_disabilitytaxcredit_fr', StringType(), True),
    StructField('esdc_copaytier', StringType(), True),
    StructField('esdc_copaytier_en', StringType(), True),
    StructField('esdc_copaytier_fr', StringType(), True),
    #StructField('esdc_provider_details', StringType(), True),
    StructField('spouse_esdc_canadianresident', StringType(), True),
    StructField('spouse_esdc_canadianresident_en', StringType(), True),
    StructField('spouse_esdc_canadianresident_fr', StringType(), True),
    #StructField('spouse_esdc_provider_details', StringType(), True),
    StructField('esdc_eligibleonfirst', DateType(), True),
    StructField('esdc_enroledon', DateType(), True),
    StructField('esdc_batchdate', DateType(), True),
    StructField('esdc_coveragestart', DateType(), True),
    StructField('esdc_coverageend', DateType(), True),
    StructField('esdc_billinggroup', StringType(), True),
    StructField('effective_date', DateType(), True),
    StructField('end_date', DateType(), True),
    StructField('created_time', TimestampType(), True),
    StructField('source', DateType(), True)
]) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sunlife Contract Milestones

# CELL ********************

readSchemaSLContractMilestones = {
    'Start_Up_Phase' : str,
    'Phase_du_Lancement' : str,
    'Milestone' : str,
    'Étape_Importante' : str,
    'Target_Date' : str, # date
    'Status' : str,
    'Statut' : str,
    'Sort' : 'Int64'
}

dateColsSLContractMilestones = [
    'Target_Date'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaSLContractMilestones = StructType ([
    StructField('Start_Up_Phase', StringType(), True),
    StructField('Phase_du_Lancement', StringType(), True),
    StructField('Milestone', StringType(), True),
    StructField('Étape_Importante', StringType(), True),
    StructField('Target_Date', DateType(), True),
    StructField('Status', StringType(), True),
    StructField('Statut', StringType(), True),
    StructField('Sort', IntegerType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sunlife Providers Billing

# CELL ********************

readSchemaSLProviderBilling = {
    'Date' : str, #date
    'Geo_Type': str,
    'Count_Type' : str,
    'PT' : str,
    'FSA' : str,
    'GP_Direct' : 'Int64',
    'HY_Direct' : 'Int64',
    'DT_Direct' : 'Int64',
    'EN_Direct' : 'Int64',
    'OM_Direct' : 'Int64',
    'OP_Direct' : 'Int64',
    'OR_Direct' : 'Int64',
    'OS_Direct' : 'Int64',
    'OT_Direct' : 'Int64',
    'PD_Direct' : 'Int64',
    'PE_Direct' : 'Int64',
    'PR_Direct' : 'Int64',
    'AN_Direct' : 'Int64',
    'TOTAL_Direct' : 'Int64',
    'GP_Not_Direct' : 'Int64',
    'HY_Not_Direct' : 'Int64',
    'DT_Not_Direct' : 'Int64',
    'EN_Not_Direct' : 'Int64',
    'OM_Not_Direct' : 'Int64',
    'OP_Not_Direct' : 'Int64',
    'OR_Not_Direct' : 'Int64',
    'OS_Not_Direct' : 'Int64',
    'OT_Not_Direct' : 'Int64',
    'PD_Not_Direct' : 'Int64',
    'PE_Not_Direct' : 'Int64',
    'PR_Not_Direct' : 'Int64',
    'AN_Not_Direct' : 'Int64',
    'TOTAL_Not_Direct' : 'Int64'
}

dateColsSLProviderBilling = [
    'Date'
]						


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaSLProviderBilling = StructType ([
    StructField('Date', DateType(), True),
    StructField('Geo_Type', StringType(), True),
    StructField('Count_Type', StringType(), True),
    StructField('PT', StringType(), True),
    StructField('FSA', StringType(), True),
    StructField('Specialty', StringType(), True),
    StructField('Direct_Billing', StringType(), True),
    StructField('Count', IntegerType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Health Canada Cost of Administration

# CELL ********************

readSchemaHCAdminCost = {
    'Date': str, # date
    'Quarter_Title': str,
    'Department': str,
    'Branch': str,
    'Funding': float,
    'Expenditure_Salary': float,
    'Expenditure_O_and_M': float
}

dateColsHCAdminCost = [
    'Date'
]	

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaHCAdminCost = StructType ([
    StructField('Date', DateType(), True),
    StructField('Quarter_Title', StringType(), True),
    StructField('Department', StringType(), True),
    StructField('Branch', StringType(), True),
    StructField('Funding', DoubleType(), True),
    StructField('Expenditure_Salary', DoubleType(), True),
    StructField('Expenditure_O_and_M', DoubleType(), True),
    StructField('Expenditure_Total', DoubleType(), True),
    StructField('Source', DateType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ESDC Contact Centre

# CELL ********************

readSchemaESDCContactCentre = {
    'Date': str, # date
    'Language': str,
    'Calls_Answered': float,
    'Calls_Answered_Within_Target': float,
    'Average_Wait_Time': float,
    'Total_Average_Wait_Time': float
}

dateColsESDCContactCentre = [
    'Date'
]	

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaESDCContactCentre = StructType ([
    StructField('Date', DateType(), True),
    StructField('Language', StringType(), True),
    StructField("Language_FR", StringType(), True),
    StructField('Calls_Answered', DoubleType(), True),
    StructField('Calls_Answered_Within_Target', DoubleType(), True),
    StructField('Average_Wait_Time', DoubleType(), True),
    StructField('Total_Average_Wait_Time', DoubleType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sun Life Cards Mailout

# CELL ********************

readSchemaSunLifeCardsMailout = {
    'Month': str,
    'English Cards': int,
    'French Cards': int,
    'Total Cards': int,
    'Total Cards Mailed within TAT': int,
    'Total Cards Mailed over TAT': int,
    'English Booklets': int,
    'French Booklets': int,
    'Total Booklets': int,
    'Total Booklets Mailed within TAT': int,
    'Total Booklets Mailed over TAT': int
}

dateColsSunLifeCardsMailout = [
    'Month'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaSunLifeCardsMailout = StructType ([
    StructField('Month', DateType(), True),
    StructField('Type', StringType(), True),
    StructField('Count', DoubleType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## CRA Co-Pay Tiers

# CELL ********************

readSchemaCRAcopay = {
    'SIN': str,
    'CO_PAY_TIER': str
}

dateColsCRAcopay = [
    
]	

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

writeSchemaCRAcopay = StructType ([
    StructField('SIN', StringType(), True),
    StructField('Member_ID', StringType(), True),
    StructField('CRA_Co_Pay_Tier', StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 
