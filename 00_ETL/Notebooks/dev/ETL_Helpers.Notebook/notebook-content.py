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

# # All

# MARKDOWN ********************

# ## Mounting Lakehouses

# CELL ********************

def append_environment_suffix(base_string, workspace_id):

    if workspace_id == "4748fbe5-9b18-4aac-9d74-f79c39ff81db":
        return f"{base_string}_DEV"
    elif workspace_id == "ec6eb7c4-c224-4908-8c57-cd348b9f58f4":
        return f"{base_string}_TEST"
    elif workspace_id == "3eb968ef-6205-4452-940d-9a6dcbf377f2":
        return f"{base_string}_PROD"
    else:
        return base_string

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_mount_paths(lakehouse_name):
    mount_point = f'/mnt_{lakehouse_name}'
    notebookutils.fs.mount(lakehouse_name, mount_point)
    fs_path = notebookutils.fs.getMountPath(mount_point)

    return {
        "fs_path": fs_path,
        "file_path": f'file:{fs_path}'
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def mount_lakehouse(lakehouse, workspace_id):

    lakehouse_name = append_environment_suffix(lakehouse, workspace_id)
    mount_paths = get_mount_paths(lakehouse_name)
    return lakehouse_name, mount_paths

# Example usage:
# workspace_id = spark.conf.get("trident.workspace.id")
# bronze_lakehouse_name, bronze_mount_paths = mount_lakehouse("Bronze_Lakehouse", workspace_id)
# silver_lakehouse_name, silver_mount_paths = mount_lakehouse("Silver_Lakehouse", workspace_id)
# gold_lakehouse_name, gold_mount_paths = mount_lakehouse("Gold_Lakehouse", workspace_id)
# dq_lakehouse_name, dq_mount_paths = mount_lakehouse("Data_Quality_Lakehouse", workspace_id)

# Read File
# tmp = pd.read_csv(dq_mount_paths['file_path'] + "/Files/2025-04-01_hc_costing_admin.csv")

# Write File
# tmp.to_csv(dq_mount_paths['fs_path'] + "/Files/test.csv", index = False)

# Read Table
# data_sources = spark.sql(f"SELECT * FROM {dq_lakehouse_name}.dbo.data_sources")

# Write Table
# spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").save(f"{dq_mount_paths['file_path']}/Tables/tmp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_date(file_name):
    # Match YYYYMMDD or YYYY-MM-DD, with optional separators around
    match = re.search(r'(?:^|[_\-. ])(\d{4}[-_]\d{2}[-_]\d{2}|\d{8})(?:[_\-. ]|$)', file_name)
    if match:
        date_str = match.group(1)
        # Convert YYYYMMDD to YYYY-MM-DD
        if '-' not in date_str:
            date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        return date_str
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 02_Bronze_to_Silver

# MARKDOWN ********************

# ## Read Files

# CELL ********************

def get_files_from_data_sources(run_type, data_sources, source, date = None):

    df_filtered = data_sources.filter(
        col('source') == source
    )

    if run_type == "single, manual":
        df_filtered = df_filtered.filter(
            col('source_date') == date
        )

    file_list = df_filtered.select("file_name").rdd.flatMap(lambda x: x).collect()

    return file_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_file_by_type(source, config, file_list):

    if config["source_type"] == "parquet":
        return read_files_parquet()

    elif config["source_type"] == "csv":
        print("Running read_files_csv()")
        return read_files_csv(source, config, file_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_files_csv(source, config, file_list):
    
    read_schema = config["read_schema"]()
    date_cols = config["date_cols"]()
    read_root = config["read_root"]

    print("Files to read: " + str(file_list))
    add = []

    for j in file_list:
        print("Reading: " + j)
        date = extract_date(j)

        if source in ["sunlife/cl90", "sunlife/cl92"]:
                tmp = pd.read_csv(f"{read_root}/{j}", dtype = read_schema, parse_dates = date_cols, skiprows = 1, skipfooter = 1, engine='python')
        else:
                tmp = pd.read_csv(f"{read_root}/{j}", dtype = read_schema, parse_dates = date_cols)

        tmp['Source'] = pd.to_datetime(date, format='%Y-%m-%d')
        add.append(tmp)

    df_raw = pd.concat(add, axis =0 ).reset_index(drop = True)
    df_raw.columns = df_raw.columns.str.replace(r'\s+', '_', regex=True)

    return df_raw        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_files_parquet(spark, rootReadFile, fileName, date):
    df_raw = spark.read.parquet(rootReadFile + fileName)
    df_raw = df_raw.withColumn('source', F.lit(date).cast('date'))
    df_raw_count = df_raw.count()
    return df_raw, df_raw_count

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Display Table

# CELL ********************

def peek_table(df):

    if isinstance(df, pd.DataFrame):

        print("Row Count: " + str(len(df.index)))
        print("Schema: \n" + str(df.dtypes))
        display(df.head(20))

    else:

        print("Row Count: " + str(df.count()))
        print("Schema:")
        df.printSchema()
        display(df.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Source Update

# CELL ********************

def list_files(base_path, source_list):
    data = []
    today = pd.Timestamp.today().normalize()  # Today's date without time

    for root, dirs, files in os.walk(base_path):
        for file in files:
            # Split the root into folder and subfolders
            rel_path = os.path.relpath(root, base_path)
            path_parts = rel_path.split(os.sep)

            #folder = path_parts[0] if len(path_parts) > 0 else ""
            source = os.path.join(*path_parts[0:]) if len(path_parts) > 1 else ""
            
            priority = 1
            write_behaviour = "overwrite"

            if source in overwrite_1_list:
                priority = 1
                write_behaviour = "overwrite"
            if source in append_1_list:
                priority = 1
                write_behaviour = "append"
            if source in append_2_list:
                priority = 2
                write_behaviour = "append"

            _, file_ext = os.path.splitext(file)
            source_date_str = extract_date(file)
            full_path = os.path.join(root, file)
            modified_date = datetime.fromtimestamp(os.path.getmtime(full_path)).date()
            trimmed_path = full_path.split("Files" + os.sep, 1)[-1] if "Files" + os.sep in full_path else full_path

            data.append({
                "full_path": trimmed_path,
                "source": source,
                "file_name": file,
                "file_type": file_ext.lower()[1:],
                "priority": priority,
                "write_behaviour": write_behaviour,
                "source_date": source_date_str,
                "modified_date": modified_date,
                "processed_status": False,
                "processed_date": None
            })

    df = pd.DataFrame(data)

    # Convert Source Date to datetime
    df["source_date"] = pd.to_datetime(df["source_date"], errors='coerce')
    df["modified_date"] = pd.to_datetime(df["modified_date"], errors='coerce')
    df["processed_date"] = pd.to_datetime(df["processed_date"], errors='coerce')

    df = df.loc[df['source'].isin(source_list)]

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
