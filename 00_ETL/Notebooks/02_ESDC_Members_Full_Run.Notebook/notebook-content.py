# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "787f65b7-02b8-4b8c-930e-3405f9a89660",
# META       "default_lakehouse_name": "Silver_Lakehouse_DEV",
# META       "default_lakehouse_workspace_id": "4748fbe5-9b18-4aac-9d74-f79c39ff81db",
# META       "known_lakehouses": [
# META         {
# META           "id": "787f65b7-02b8-4b8c-930e-3405f9a89660"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Setup

# MARKDOWN ********************

# ## Parameters & data storage connections

# PARAMETERS CELL ********************

counts = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sparkContext.setCheckpointDir("Files/temp_checkpoints")

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

elif workspace_id == "ec6eb7c4-c224-4908-8c57-cd348b9f58f4": #test
    bronze_lakehouse_id = "c2b72635-1f18-4f8b-909a-94726216cc87"

elif workspace_id == "3eb968ef-6205-4452-940d-9a6dcbf377f2": #prod
    bronze_lakehouse_id = "dee2baf0-f8c0-4682-9154-bf0a213e41ee"

notebookutils.fs.mount(f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}", "/lakehouse/Bronze_Lakehouse")

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

%run DEV_02_Bronze_to_Silver_Helper

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Set variables

# CELL ********************

# rootFileSearch - populates df_source with files that are in this directory
rootFileSearch = notebookutils.fs.getMountPath("/lakehouse/Bronze_Lakehouse/Files/ESDC/Members/")

# rootReadFile is used to read the file later on
rootReadFile = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{bronze_lakehouse_id}/Files/ESDC/Members/"

apply_transformations = getattr(ESDCMembersWrangler, 'apply_transformations')
writePath = "Tables/esdc/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create Source Table

# CELL ********************

# Function to extract and format the last 8-digit sequence as a date
def extract_last_date_formatted(input_str):
    reg_match = re.search(r"(\d{8})(?!.*\d{8})", input_str)
    if reg_match:
        last_date = reg_match.group(1)
        return f"{last_date[:4]}-{last_date[4:6]}-{last_date[6:]}"  # Reformat as YYYY-MM-DD
    return None  # Return None if no match is found

# Function to subtract one day from a date string
def subtract_one_day(date_str):
    if date_str is not None:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")  # Convert to datetime
        new_date_obj = date_obj - timedelta(days=1)         # Subtract one day
        return new_date_obj.strftime("%Y-%m-%d")            # Convert back to string
    return None  # Handle None values gracefully

fileNames = glob.glob(os.path.join(rootFileSearch, f"CDCP_PROD_ESDC_Eligible*.parquet"))
df_source = pd.DataFrame(fileNames, columns = ['Full_Name'])
df_source['Base_Name'] = df_source['Full_Name'].apply(os.path.basename)
df_source['source_date'] = df_source['Base_Name'].apply(extract_last_date_formatted)
df_source['end_date'] = df_source['source_date'].apply(subtract_one_day)
df_source.loc[df_source['Base_Name'].str.contains("Full"), 'end_date'] = '9999-12-31'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_source)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Read, transform, append, and save key numbers

# CELL ********************

for index, row in df_source.iterrows():
    #if index == 10: break
    if index == 0:
        
        # Read initial file
        df_raw = spark.read.parquet(rootReadFile + row['Base_Name'])
        df_raw = df_raw.withColumn('source', F.lit(row['source_date']).cast('date'))
        df_raw_count = df_raw.count()
        df_raw_upsert_duplicate_count = (
            df_raw.groupBy('uniqueid')         
            .agg(F.count("*").alias("count"))
            .filter(col("count") > 1)
            .count()
        ) 

        # Extract provider details
        df_esdc_provider_details = ESDCMembersWrangler._flatten_nested_column_into_new_df(df = df_raw, explodeCol = 'esdc_provider_details')
        df_spouse_esdc_provider_details = ESDCMembersWrangler._flatten_nested_column_into_new_df(df = df_raw, explodeCol = 'spouse_esdc_provider_details')

        # Transform initial file
        df_transformed = apply_transformations(df_raw, runType = "all", data_date = "2024-11-14")
        df_transformed_count = df_transformed.count()
        df = df_transformed

        # Add key numbers to df_source
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Total_Before'] = 0
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Delta_Raw'] = df_raw_count
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Delete_Raw'] = 0
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Delete_Without_Match'] = 0
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Delete_Without_Match_Detailed'] = 0
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Delete_Without_Match_No_Duplicates'] = 0
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Delete_Planned'] = 0
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Delete_Actual'] = 0
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Upsert_Raw'] = df_raw_count
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Upsert_Duplicates'] = df_raw_upsert_duplicate_count
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Upsert_Planned'] = df_raw_count - df_raw_upsert_duplicate_count
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Upsert_Actual'] = df_transformed_count
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Total_Duplicate_DFP'] = 0
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Total_Planned'] = df_transformed_count
        df_source.loc[df_source['source_date'] == "2024-11-14", 'Total_Actual'] = df_transformed_count

        m = df_source.select_dtypes(np.number)
        df_source[m.columns]= m.round().astype('Int64')

    else:
        if index % 40 == 0:
            df = df.checkpoint()
            df.count()
        print("Starting: " + str(row['Base_Name']))
        if counts:
            total_before_count = df.count()

        # Read file
        df_raw = spark.read.parquet(rootReadFile + row['Base_Name'])
        df_raw = df_raw.withColumn('source', F.lit(row['source_date']).cast('date'))
        

        # Extract provider details
        df_esdc_provider_details_temp = ESDCMembersWrangler._flatten_nested_column_into_new_df(df = df_raw, explodeCol = 'esdc_provider_details')
        df_spouse_esdc_provider_details_temp = ESDCMembersWrangler._flatten_nested_column_into_new_df(df = df_raw, explodeCol = 'spouse_esdc_provider_details')

        df_esdc_provider_details = df_esdc_provider_details.union(df_esdc_provider_details_temp)
        df_spouse_esdc_provider_details = df_spouse_esdc_provider_details.union(df_spouse_esdc_provider_details_temp)


        # Deal with Deletes
        delete = df_raw.filter(col('dfp_change') == "delete")
        

        # Split dfp_id into an array so it can be exploded into multiple rows
        df_old_split = df.withColumn("dfp_array", F.split(F.col("dfp_incremental_id"), "\\|")) 
        df_old_exploded = df_old_split.withColumn("dfp_exploded", F.explode(F.col("dfp_array")))

        # Checks for number of deletes that don't have a dfp in df_old
        delete_without_match = delete.join(df, on = 'dfp_incremental_id', how = "left_anti")
        delete_without_match_values = delete_without_match.selectExpr("dfp_incremental_id as dfp_incremental_id_join")
        
        
        # Join exploded df to non-matching delete values to double check numbers
        delete_no_match_joined = df_old_exploded.join(delete_without_match_values, df_old_exploded.dfp_exploded == delete_without_match_values.dfp_incremental_id_join, "left")
        delete_no_match_filtered = delete_no_match_joined.filter(col("dfp_incremental_id_join").isNotNull())
        
        delete_no_match_distinct = delete_no_match_filtered.dropDuplicates(["uniqueid"])
        

        # Join exploded df to all delete values to update end_date
        delete_values = delete.selectExpr("dfp_incremental_id as dfp_incremental_id_join")
        delete_joined = df_old_exploded.join(delete_values, df_old_exploded.dfp_exploded == delete_values.dfp_incremental_id_join, "left")

        # Update end_date if there is a corresponding delete, then drop intermediate rows and duplicates from earlier explosion
        df = delete_joined.withColumn(
            'end_date',
            F.when(F.col('dfp_incremental_id_join').isNotNull(), F.lit(row['end_date']).cast("date")).otherwise(F.col('end_date'))
        ).drop('dfp_incremental_id_join', 'dfp_array', 'dfp_exploded') \
        .dropDuplicates(["dfp_incremental_id"])

        


        # Deal with Upserts
        upsert = df_raw.filter(col('dfp_change') == "upsert")
        if counts:
            upsert_raw_count = upsert.count()
            upsert_duplicate_count = (
                upsert.groupBy('uniqueid')
                .agg(F.count("*").alias("count"))
                .filter(col("count") > 1)
                .count()
            )

        # Transform upserts
        upsert_transformed = apply_transformations(upsert, runType = "single", data_date = row['source_date'])
        

        df = df.union(upsert_transformed)
        
        if counts:
            total_dupe_count = (
                df.groupBy('dfp_incremental_id')
                .agg(F.count("*").alias("count"))
                .filter(col("count") > 1)
                .count()
            )

        # Define a window partitioned by ID and ordered by effective_date descending
        window_spec = Window.partitionBy('dfp_incremental_id').orderBy(F.col('effective_date').desc())

        # Create a rank column
        df = df.withColumn('rank', F.row_number().over(window_spec))

        # Concatenate ID and rank for rows with rank > 1
        df = df.withColumn(
            'dfp_incremental_id',
            F.when(F.col('rank') == 1, F.col('dfp_incremental_id'))  # Keep original ID for most recent row
            .otherwise(F.concat(F.col('dfp_incremental_id'), F.lit("-"), F.col('rank')))
        ).drop('rank')


        
        if counts:
            delta_raw_count = df_raw.count()
            delete_raw_count = delete.count()
            delete_without_match_count = delete_without_match_values.count()
            delete_no_match_detailed_count = delete_no_match_filtered.count()
            delete_no_match_distinct_count = delete_no_match_distinct.count()
            delete_actual_count = df.filter(col('end_date') == row['end_date']).count()
            upsert_actual_count = upsert_transformed.count()
            total_actual_count = df.count()

            df_source.loc[df_source['source_date'] == row['source_date'], 'Total_Before'] = total_before_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Delta_Raw'] = delta_raw_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Delete_Raw'] = delete_raw_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Delete_Without_Match'] = delete_without_match_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Delete_Without_Match_Detailed'] = delete_no_match_detailed_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Delete_Without_Match_No_Duplicates'] = delete_no_match_distinct_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Delete_Planned'] = delete_raw_count - delete_no_match_distinct_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Delete_Actual'] = delete_actual_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Upsert_Raw'] = upsert_raw_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Upsert_Duplicates'] = upsert_duplicate_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Upsert_Planned'] = upsert_raw_count - upsert_duplicate_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Upsert_Actual'] = upsert_actual_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Total_Duplicate_DFP'] = total_dupe_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Total_Planned'] = total_before_count + upsert_actual_count
            df_source.loc[df_source['source_date'] == row['source_date'], 'Total_Actual'] = total_actual_count


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Review Key Numbers

# CELL ********************

#print(df.count())
#display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_source)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Save to Silver

# CELL ********************

df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(writePath + "dim_members")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_esdc_provider_details.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(writePath + "dim_insurance_details")
df_spouse_esdc_provider_details.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(writePath + "dim_insurance_details_spouse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

    data_sources_update = spark_df_joined.withColumn(
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

data_sources_update.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{mount_path}/Tables/data_sources")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
