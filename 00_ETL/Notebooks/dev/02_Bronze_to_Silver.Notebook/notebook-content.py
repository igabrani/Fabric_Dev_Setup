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

# # Setup

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

# Set parameters to move and transform data from bronze to silver

# Set data source for processing
source = "sunlife/contact_centre_stats"

#file_name = "CL90 - Claims Listing_2025-05-09.csv"
file_name = None

# Date part of .csv str name
date = "2025-05-09"

# Run "single" or "all" files in dir
run_type = "all"

# "manual", "pipeline"
trigger = "manual"

# Workspace {test = creates a deep copy of df_raw before transforming, so df_raw can be tested on multiple times, 
#            prod = transforms df_raw irreversibly, which is faster in production}
environment = "test"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helper Notebooks

# CELL ********************

%run 00_Load_Packages

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ETL_Helpers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = spark.conf.get("trident.workspace.id")
bronze_lakehouse_name, bronze_mount_paths = mount_lakehouse("Bronze_Lakehouse", workspace_id)
silver_lakehouse_name, silver_mount_paths = mount_lakehouse("Silver_Lakehouse", workspace_id)
dq_lakehouse_name, dq_mount_paths = mount_lakehouse("Data_Quality_Lakehouse", workspace_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if trigger == "manual":
    notebookutils.notebook.run("01_Data_Source_Update", 600, {"run_type": "update"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run DEV_02_Bronze_to_Silver_Helper

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read Data

# CELL ********************

config = registry.get(source)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if (run_type == "all") | (trigger == "manual"):
    data_sources = spark.sql(f"SELECT * FROM {dq_lakehouse_name}.data_sources.data_sources").orderBy(['priority', 'write_behaviour', 'source', 'source_date'])
    file_list = get_files_from_data_sources(run_type, data_sources, source, date)
else:
    file_list = [file_name]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_raw = read_file_by_type(source, config, file_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if environment == "test":
    peek_table(df_raw)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Validate Data (schema)

# MARKDOWN ********************

# # Apply Transformations

# MARKDOWN ********************

# ## Regular Transformations

# CELL ********************

if environment == "test": 
    df_transformed = df_raw.copy()
else:
    df_transformed = df_raw

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if config["source_type"] == "csv":
    
    df_transformed = config["apply_transformations"](df_transformed)

    if config["apply_joins"]:
        df_transformed = config["apply_joins"](df_transformed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if environment == "test":
    peek_table(df_transformed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Supplemental Transformations

# MARKDOWN ********************

# ### ESDC Members

# CELL ********************

if source == "esdc/members_eligible" and runType == "single":

    df_old = spark.sql(f"SELECT * FROM {silver_lakehouse_name}.esdc.dim_members")

    # Deal with Deletes
    delete = df_raw.filter(col('dfp_change') == "delete")
    

    # Split dfp_id into an array so it can be exploded into multiple rows
    df_old_split = df_old.withColumn("dfp_array", F.split(F.col("dfp_incremental_id"), "\\|")) 
    df_old_exploded = df_old_split.withColumn("dfp_exploded", F.explode(F.col("dfp_array")))
    

    # Join exploded df to all delete values to update end_date
    delete_values = delete.selectExpr("dfp_incremental_id as dfp_incremental_id_join")
    delete_joined = df_old_exploded.join(delete_values, df_old_exploded.dfp_exploded == delete_values.dfp_incremental_id_join, "left")

    # Update end_date if there is a corresponding delete, then drop intermediate rows and duplicates from earlier explosion
    df_old = delete_joined.withColumn(
        'end_date',
        F.when(F.col('dfp_incremental_id_join').isNotNull(), F.lit(end_date).cast("date")).otherwise(F.col('end_date'))
    ).drop('dfp_incremental_id_join', 'dfp_array', 'dfp_exploded') \
    .dropDuplicates(["dfp_incremental_id"])


    # Deal with Upserts
    upsert = df_raw.filter(col('dfp_change') == "upsert")

    # Transform upserts
    upsert_transformed = apply_transformations(upsert, runType = "single", data_date = date)
    
    df_transformed = df_old.union(upsert_transformed)


    # Define a window partitioned by ID and ordered by effective_date descending
    window_spec = Window.partitionBy('dfp_incremental_id').orderBy(F.col('effective_date').desc())

    # Create a rank column
    df_transformed = df_transformed.withColumn('rank', F.row_number().over(window_spec))

    # Concatenate ID and rank for rows with rank > 1
    df_transformed = df_transformed.withColumn(
        'dfp_incremental_id',
        F.when(F.col('rank') == 1, F.col('dfp_incremental_id'))  # Keep original ID for most recent row
        .otherwise(F.concat(F.col('dfp_incremental_id'), F.lit("-"), F.col('rank')))
    ).drop('rank')

    #print(df_transformed.count())
    #df_transformed.printSchema()
    #display(df_transformed.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if source == "esdc/members_eligible":
    df_esdc_provider_details = ESDCMembersWrangler._flatten_nested_column_into_new_df(df = df_raw, explodeCol = 'esdc_provider_details')
    df_spouse_esdc_provider_details = ESDCMembersWrangler._flatten_nested_column_into_new_df(df = df_raw, explodeCol = 'spouse_esdc_provider_details')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### PCCF FSA Table

# CELL ********************

if source == "statcan/pccf":
    df_fsa = wrangler_statcan_pccf._create_FSA_table(df = df_transformed)
    display(df_fsa)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### PP08 compare and append dropped rows

# CELL ********************

if source == "sunlife/pp08":

    df_old = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.sunlife.sunlife_pp08').toPandas()

    # Anti join
    outer = df_transformed.merge(df_old, how = 'outer', on = 'Provider_ID', indicator = True)
    new_entries = outer[(outer._merge == 'left_only')].drop('_merge', axis = 1)
    dropped_entries = outer[(outer._merge == 'right_only')].drop('_merge', axis = 1)

    # Sort and rename columns of dropped_entries
    keepCols = [
        'Provider_ID',
        'Provider_PT_y',
        'Provider_PT_Alt_y',
        'Provider_Postal_Code_y',
        'Provider_FSA_y',
        'Provider_Area_Type_y',
        'Participating_Date_y',
        'Enrolled_Status_y',
        'Specialty_y',
        'Language_y',
        'Direct_Billing_y',
        'Source_y'
    ]

    renameCols = {
        'Provider_PT_y': 'Provider_PT',
        'Provider_PT_Alt_y': 'Provider_PT_Alt',
        'Provider_Postal_Code_y': 'Provider_Postal_Code',
        'Provider_FSA_y': 'Provider_FSA',
        'Provider_Area_Type_y': 'Provider_Area_Type',
        'Participating_Date_y': 'Participating_Date',
        'Enrolled_Status_y': 'Enrolled_Status',
        'Specialty_y': 'Specialty',
        'Language_y': 'Language',
        'Direct_Billing_y': 'Direct_Billing',
        'Source_y': 'Source'
    }

    dropped_entries = dropped_entries[keepCols].rename(columns = renameCols)
    dropped_entries['Participating_Date'] = pd.to_datetime(dropped_entries['Participating_Date'])
    dropped_entries['Source'] = pd.to_datetime(dropped_entries['Source'])

    dropped_entries.loc[dropped_entries['Enrolled_Status'], 'Source'] = pd.to_datetime(date)
    dropped_entries['Enrolled_Status'] = False

    # Display new and dropped entries
    print("New Entries: " + str(len(new_entries.index)))
    display(new_entries)
    print("Dropped Entries: " + str(len(dropped_entries.index)))
    display(dropped_entries)

    # Combining new entries to df
    df_appended = pd.concat([df_transformed, dropped_entries], ignore_index = True, sort = False)
    print("Appended Length: " + str(len(df_appended)))

    # Checking if there is any duplication in new id
    duplicateRows = df_appended[df_appended.duplicated(['Provider_ID'])]
    print("Duplicate Rows: " + str(len(duplicateRows)))

    df_transformed = df_appended

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Validate Data (transformations)

# MARKDOWN ********************

# # Write to Delta Table in Silver Lakehouse

# MARKDOWN ********************

# ## Transform Pandas DF into Spark DF

# CELL ********************

if config["source_type"] == "parquet":
    spark_df = df_transformed
elif config["source_type"] == "csv":
    spark_df = spark.createDataFrame(df_transformed, config["write_schema"]())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if environment == "test":
    peek_table(spark_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Silver Lakehouse - default tables

# CELL ********************

if run_type == "single":
    spark_df.write.format("delta").mode(config["write_behaviour"]).option("overwriteSchema", "True").save(config["write_path"]())
elif run_type == "all":
    spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(config["write_path"]())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Silver Lakehouse - specific tables

# CELL ********************

if source == "hc/procedure_codes":
    spark_df_os_codes = spark_df.filter((spark_df.Specialty == "OS") & (spark_df.QC_Flag == "CAN")).select(spark_df.Code_Base)
    spark_df_os_codes.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/hc/hc_procedure_codes_os")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if source == "esdc/members_eligible":
    df_esdc_provider_details.write.format("delta").mode("append").option("overwriteSchema", "True").save("Tables/esdc/dim_insurance_details")
    df_spouse_esdc_provider_details.write.format("delta").mode("append").option("overwriteSchema", "True").save("Tables/esdc/dim_insurance_details_spouse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if source == "statcan/pccf":
    spark_df_fsa = spark.createDataFrame(df_fsa)
    spark_df_fsa.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save("Tables/statcan/dim_fsa")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Update Data Quality Lakehouse

# CELL ********************

if trigger == "manual":

    if run_type == "all":
        data_sources_update = data_sources.withColumn(
            'processed_status',
            F.when(col('source') == source, True)
            .otherwise(col('processed_status'))
        )

        data_sources_update = data_sources_update.withColumn(
            'processed_date',
            F.when(col('source') == source, F.current_date())
            .otherwise(col('processed_date'))
        )

    elif run_type == "single":
        data_sources_update = data_sources.withColumn(
            'processed_status',
            F.when(col('full_path') == full_path, True)
            .otherwise(col('processed_status'))
        )

        data_sources_update = data_sources_update.withColumn(
            'processed_date',
            F.when(col('full_path') == full_path, F.current_date())
            .otherwise(col('processed_date'))
        )

    display(data_sources_update.filter(col('source') == source))
    display(data_sources.filter(col('source') == source))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if trigger == "manual":
    data_sources_update.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{dq_mount_paths['file_path']}/Tables/data_sources/data_sources")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
