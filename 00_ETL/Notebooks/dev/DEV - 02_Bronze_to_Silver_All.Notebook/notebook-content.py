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

# CELL ********************

import pprint

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

overwrite_1_list = [
    "cra/copay_tiers",
    "esdc/contact_centre_stats",
    "esdc/fsa_applications",
    "esdc/pt_applications",
    "esdc/fsa_enrolled",
    "esdc/pt_enrolled",
    "hc/cost_of_administration",
    "hc/cost_of_sunlife_contract",
    "hc/eligible_population_estimates",
    #"hc/procedure_codes", <- fix naming
    "hc/sunlife_contract_milestones",
    #"ohds/grouping",
    #"ohds/mapping",
    #"statcan/pccf", <- fix naming and alt table
    "sunlife/cards_mailout",
    #"sunlife/fee_guide_universe",
    #"sunlife/overpayments_universe",
    #"sunlife/pp08", <- fix alt table
    "sunlife/procedure_code_descriptions",
    "sunlife/provider_billing",
    #"sunlife/providers_universe",
    "sunlife/reason_and_remark_codes",
]

append_1_list = [
    #"hc/oh_ip/change_request",
    #"hc/oh_ip/financial_actuals",
    #"hc/oh_ip/financial_estimates",
    #"hc/oh_ip/risk",
    #"hc/oh_ip/schedule_baseline",
    #"hc/oh_ip/schedule_revised",
    "sunlife/contact_centre_stats",
    "sunlife/fi02",
    #"sunlife/financial_universe",
    #"sunlife/members_universe"
]

priority_1_list = overwrite_1_list + append_1_list

append_2_list = [
    #"esdc/members_eligible", <- fix parquet and both run_types
    #"esdc/members_ineligible",
    "sunlife/cl90",
    "sunlife/cl92",
    #"sunlife/claims_universe",
    #"sunlife/estimates_universe"
]

all_sources_list = priority_1_list + append_2_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_activity(name, source, depends_on=[]):
    
    run_type = "single" if source == "sunlife/pp08" else "all"
    
    return {
        "name": name,
        "path": "02_Bronze_to_Silver",
        "timeoutPerCellInSeconds": 600,
        "dependsOn": depends_on,
        "args": {
            "source": source,
            "run_type": run_type,
            "trigger": "pipeline",
            "environment": "prod"
        }
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_activities = []

for src in priority_1_list:

    name = "02_Bronze_to_Silver: " + src
    activity = create_activity(name, src)
    all_activities.append(activity)

prereq_names = [a["name"] for a in all_activities]

for src in append_2_list:
    
    name = "02_Bronze_to_Silver: " + src
    activity = create_activity(name, src, depends_on=prereq_names)
    all_activities.append(activity)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAG = {
    "activities": all_activities,
    "timeoutInSeconds": 43200, # max timeout for the entire pipeline, default to 12 hours
    "concurrency": 50 # max number of notebooks to run concurrently, default to 50, 0 means unlimited
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(notebookutils.notebook.validateDAG(DAG))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pprint.pprint(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run 01_Data_Source_Update

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

runMultiple_return = notebookutils.notebook.runMultiple(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if runMultiple_return:

    data_sources_update = spark_df.withColumn(
        'processed_status',
        F.when(col('source').isin(all_sources_list), True)
        .otherwise(col('processed_status'))
    )

    data_sources_update = data_sources_update.withColumn(
        'processed_date',
        F.when(col('source').isin(all_sources_list), F.current_date())
        .otherwise(col('processed_date'))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(data_sources_update.filter(~(col('source').isin(all_sources_list))))
display(data_sources_update.filter(col('source').isin(all_sources_list)))
display(spark_df.filter(col('source').isin(all_sources_list)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data_sources_update.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{dq_mount_paths['file_path']}/Tables/data_sources/data_sources")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
