# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1e63eaee-a408-4c2e-975e-e4ab7f8b08c9",
# META       "default_lakehouse_name": "Data_Quality_Lakehouse_PROD",
# META       "default_lakehouse_workspace_id": "3eb968ef-6205-4452-940d-9a6dcbf377f2",
# META       "known_lakehouses": [
# META         {
# META           "id": "1e63eaee-a408-4c2e-975e-e4ab7f8b08c9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Classes and Functions

# CELL ********************

%run 00_Load_Packages

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyarrow as pa
import os
import pandas as pd
from notebookutils import fs
from pandas.api.types import infer_dtype
import pyarrow.compute as pc
from IPython.display import clear_output
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
import numpy as np
from pyspark.sql.functions import current_timestamp, from_utc_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType, BooleanType, TimestampType, LongType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
import io

# Create a StringIO stream to capture log messages
log_stream = io.StringIO()

# Create a logger
logger = logging.getLogger()

# Set logging level to DEBUG (ensure it's the log level, not the method)
logger.setLevel(logging.DEBUG)

# Remove default handler that writes to console (stdout)
if logger.hasHandlers():
    logger.handlers.clear()

# Create a stream handler that writes to the StringIO stream (no console output)
handler = logging.StreamHandler(log_stream)

# Set the logging format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)

# Add a NullHandler to suppress any logging errors
logger.addHandler(logging.NullHandler())  # This suppresses error messages from the logger

# Function to get all logged messages
def get_log_output():
    return log_stream.getvalue()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Environment Set Up

# CELL ********************

class EnvironmentConfig:
    """Class to set workspace and lakehouse IDs based on the environment."""

    def __init__(self, lakehouse_name):
        self.lakehouse_name = lakehouse_name.lower().replace("_", " ") if isinstance(lakehouse_name, str) else ""
        self.workspace_id = self.set_workspace_id()
        self.lakehouse_id = self.set_lakehouse_id()
        self.lakehouse_alias = next((lakehouse.title().replace(" ", "_") for lakehouse in ["bronze", "silver", "gold", "data quality"] if lakehouse in self.lakehouse_name), "unknown")
        self.mount_path = f"/lakehouse/{self.lakehouse_alias}_Lakehouse"

    def set_workspace_id(self):
        """Set workspace ID based on the environment."""
        if "dev" in self.lakehouse_name:
            return "4748fbe5-9b18-4aac-9d74-f79c39ff81db"
        elif "test" in self.lakehouse_name:
            return "ec6eb7c4-c224-4908-8c57-cd348b9f58f4"
        elif "prod" in self.lakehouse_name:
            return "3eb968ef-6205-4452-940d-9a6dcbf377f2"
        else:
            return "Lakehouse name unknown."

    def set_lakehouse_id(self):
        """Set lakehouse IDs for bronze, silver, and gold based on the workspace."""
        lakehouse_mapping = {
            "dev": {
                "bronze": "b4c04e3f-37d8-4e5f-8f13-f8a76b3eefbe",
                "silver": "787f65b7-02b8-4b8c-930e-3405f9a89660",
                "gold": "30403899-73c8-4ff0-87e6-107b5a167e84",
                "data quality": ""
            },
            "test": {
                "bronze": "c2b72635-1f18-4f8b-909a-94726216cc87",
                "silver": "",
                "gold": "",
                "data quality": ""
            },
            "prod": {
                "bronze": "dee2baf0-f8c0-4682-9154-bf0a213e41ee",
                "silver": "911ba9c9-2599-42c5-b72c-30050958d181",
                "gold": "aee93c31-04bd-4a97-9465-a0464f0a841a",
                "data quality": "1e63eaee-a408-4c2e-975e-e4ab7f8b08c9"
            }
        }
        
        # Determine which lakehouse (bronze, silver, gold, or data quality) to use based on the value
        for lakehouse in ["bronze", "silver", "gold", "data quality"]:
            if lakehouse in self.lakehouse_name:
                if "dev" in self.lakehouse_name:
                    return lakehouse_mapping["dev"][lakehouse]
                elif "test" in self.lakehouse_name:
                    return lakehouse_mapping["test"][lakehouse]
                elif "prod" in self.lakehouse_name:
                    return lakehouse_mapping["prod"][lakehouse]
    

    def mount_lakehouse(self):
        """Mount the selected lakehouse based on workspace and lakehouse ID."""
        # Extract the lakehouse type (bronze, silver, gold, "data quality") from the value
        #lakehouse_alias = next((lakehouse_name.title().replace(" ", "_") for lakehouse in ["bronze", "silver", "gold", "data quality"] if lakehouse in self.lakehouse_name), "unknown")

        # Capitalize the lakehouse type (Silver, Gold, or Bronze)
        #mount_path = f"/lakehouse/{self.lakehouse_alias}_Lakehouse"
        
        # Construct the mount command
        mount_command = f"notebookutils.fs.mount(f'abfss://{self.workspace_id}@onelake.dfs.fabric.microsoft.com/{self.lakehouse_id}', '{self.mount_path}')"
        exec(mount_command)  # Execute the mount command

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dataset Importer

# CELL ********************

class DatasetImporter:
    """
    A class to handle the process of setting parameters and importing datasets.
    """

    # File naming templates
    File_Name_Templates = {
        "CL90": "CL90 - Claims Listing_{date}.csv",
        "CL92": "CL92 - Estimates Listing_English_{date}.csv", #-- beyond April 2nd
        #"CL92": "CL92 – Estimates Listing_English_{date}.csv",
        "FI02": "FI02_Claim_Funding_Expenditure_{date}.csv",
        "CL90 - OLAP": "{date}_claims_universe.csv",
        "CL92 - OLAP": "{date}_estimates_universe.csv",
        "FI02 - OLAP": "{date}_financial_universe.csv",
    }

    def __init__(self, source):
        local_vars = globals()  # Get the global namespace
        
        # Extract variables dynamically based on source group
        if source == 'src1':
            src_vars = {key: local_vars[key] for key in local_vars if key.startswith('src1')}
        elif source == 'src2':
            src_vars = {key: local_vars[key] for key in local_vars if key.startswith('src2')}
        else:
            raise ValueError("Invalid source. Choose 'src1' or 'src2'.")

        # Ensure that the number of variables is exactly 6
        if len(src_vars) != 6:
            raise ValueError(f"Expected 6 variables, but found {len(src_vars)} in '{source}' group. Please check the source variables.")
        
        # Store extracted parameters as a tuple
        self.params = tuple(src_vars.values())
        
        # Ensure self.params is not None and contains exactly 6 values
        if self.params is None or len(self.params) != 6:
            raise ValueError(f"params is not correctly populated. Found: {self.params}")

        # Assign values to instance variables
        try:
            (
                self.lvl1_loc,
                self.lvl2_loc,
                self.lakehouse_name,
                self.object_type,
                self.run_type,
                self.date,
            ) = self.params
        except ValueError:
            raise ValueError("Unable to unpack parameters. Ensure that 'params' contains exactly 6 values.")

        # Initialize EnvironmentConfig and call mount_lakehouse to set the mount_path
        self.env_init = EnvironmentConfig(self.lakehouse_name)  # EnvironmentConfig is already defined
        self.env_init.mount_lakehouse()  # Set the mount_path dynamically

        clear_output(wait=True)
        
        # Now set self.mount_path using the value from the EnvironmentConfig
        self.mount_path = self.env_init.mount_path  # Access mount_path from env_init

        self.root, self.object_name = self.set_parameters()
        self.df_raw = None  # Initialize df_raw attribute
        self.spark = SparkSession.builder.appName("DatasetImporter").getOrCreate()  # Initialize SparkSession

    def set_parameters(self):
        """Sets the parameters for file paths based on the object type."""
        try:
            if self.object_type == "files":
                root = fs.getMountPath(f"{self.mount_path}/{self.object_type.capitalize()}/{self.lvl1_loc}/{self.lvl2_loc}/")
                # Use the template if lvl2_loc exists in the dictionary, otherwise use a default format
                object_name = (
                    self.File_Name_Templates[self.lvl2_loc].format(date=self.date)
                    if self.lvl2_loc in self.File_Name_Templates
                    else f"{self.lvl2_loc}.csv"
                )
            elif self.object_type == "tables":
                root = fs.getMountPath(f"{self.mount_path}/{self.object_type.capitalize()}/{self.lvl1_loc}/")
                object_name = self.lvl2_loc.lower()  # lvl2_loc is used as the table name
            else:
                logger.debug(self.object_type, self.lvl1_loc, self.lvl2_loc)
                raise ValueError(f"Unsupported object type: {self.object_type}")
            return root, object_name
        except Exception as e:
            logger.debug(f"[DEBUG] Error setting data import parameters: {str(e)}")


    def import_delta_table(self):
        """Imports a Delta table and converts it to a pandas DataFrame."""
        file_path = 'abfss://CDCP@onelake.dfs.fabric.microsoft.com/Silver_Lakehouse_PROD.Lakehouse/Tables/sunlife/' + self.object_name
        self.df_raw = self.spark.read.format("delta").load(file_path)

    def import_csv(self):
        """Imports the dataset based on the set parameters."""
        try:
            if self.run_type == "all":
                # Read all .csv files in directory and append them to a master DataFrame
                list_files = os.listdir(self.root)
                list_csv = sorted([f for f in list_files if f.endswith('.csv')])

                data_frames = []
                for file in list_csv[1:2]:  # Adjust this range as needed
                    file_path = os.path.join(self.root, file)
                    if self.lvl2_loc in ["CL90", "CL92"]:
                        tmp = pd.read_csv(file_path, skiprows=1, skipfooter=1, engine='python')
                    else:
                        tmp = pd.read_csv(file_path)
                    tmp['Source'] = pd.to_datetime(file[-14:-4], format='%Y-%m-%d', errors='coerce')
                    data_frames.append(tmp)

                self.df_raw = pd.concat(data_frames, axis=0).reset_index(drop=True) if data_frames else pd.DataFrame()

            elif self.run_type == "single":
                file_path = os.path.join(self.root, self.object_name)
                #logger.debug(self.root , self.object_name)
                if self.lvl2_loc in ["CL90", "CL92"]:
                    self.df_raw = pd.read_csv(file_path, skiprows=1, skipfooter=1, engine='python')
                elif self.lvl2_loc in ["CL90 - OLAP", "CL92 - OLAP"]:
                    self.df_raw = pd.read_csv(file_path, engine='python')
                else:
                    #logger.debug(file_path)
                    self.df_raw = pd.read_csv(file_path)
            else:
                raise ValueError(f"Unsupported run type: {self.run_type}")
        except Exception as e:
            logger.debug(f"[DEBUG] Error importing csv: {str(e)}")

    def import_dataset(self):
        """Executes the full dataset import process."""
        try:
            if self.object_type.lower() == 'files':
                self.import_csv()
                return self.df_raw, self.object_name
            elif self.object_type.lower() == 'tables':
                self.import_delta_table(), self.object_name
                return self.df_raw
            else: 
                raise ValueError(f"Unsupported object type: {self.object_type}")
        except Exception as e:
            logger.debug(f"[DEBUG] Error importing Dataset: {str(e)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DataFrame Cleaning

# CELL ********************

class DataFrameCleaning:
    def __init__(self, df):
        """
        Initialize with a DataFrame.
        """
        self.df_raw = df

    @staticmethod
    def clean_values(val):
        """Cleans individual cell values by stripping spaces and replacing null-like values."""
        if isinstance(val, str):  
            original_val = val  # Store the original value for debugging
            val = val.strip()  # Strip any leading/trailing spaces
            
            if val in ['N/A', 'NULL', '']:  # If the value is a placeholder for missing data
                return np.nan  # Replace with np.nan (pandas missing value)
        
        return val  # Return the cleaned value

    def remove_repeated_headers(self):
        """
        Removes rows that are identical to the DataFrame's column names.

        Parameters:
        df (pd.DataFrame): The input DataFrame with repeated headers

        Returns:
        pd.DataFrame: The cleaned DataFrame without repeated headers
        """
        # Convert column names to a set for comparison
        column_set = set(self.df_raw.columns)
        logger.debug("[DEBUG] Column names identified:", self.df_raw.columns)

        # Print the first few rows before removal of repeated headers
        logger.debug("[DEBUG] Data before removing repeated headers:")

        # Filter out rows where all values match the column names
        self.df_raw = self.df_raw[~self.df_raw.apply(lambda row: set(row) == column_set, axis=1)]
        
        # Reset index after removing repeated headers
        self.df_raw = self.df_raw.reset_index(drop=True)

        # Print the first few rows after removal of repeated headers
        logger.debug("[DEBUG] Data after removing repeated headers:")


    def clean_column_name(self):
        """Preprocess the DataFrame by standardizing column names, cleaning values, and converting types."""
        # Strip and normalize column names
        logger.debug("[DEBUG] Original column names:", self.df_raw.columns)
        
        self.df_raw.columns = self.df_raw.columns.str.strip().str.replace(r"\s+", " ", regex=True)
        logger.debug("[DEBUG] Cleaned column names:", self.df_raw.columns)

        # Apply value cleaning to entire DataFrame
        logger.debug("[DEBUG] Applying value cleaning...")
        self.df_raw = self.df_raw.applymap(self.clean_values)

        # Rename columns based on lvl2_loc condition
        column_map_dict = {
            #CL90 & CL92
            'Age': 'Member Age', 
            'Co-Pay(Plan ID)': 'Co-Pay (Plan ID) Category', 
            'Member Postal Code': 'Member Postal Code / Zip Code', 
            'Member Province': 'Member Province/Territory', 
            'Procedure Code': 'Procedure Code Submitted', 
            'Provider Province': 'Provider Province/Territory', 
            'Specialty': 'Provider Specialty',
            #FI02
            'Member Province/Territories': 'Member Province/Territory',
            'Provider Number': 'Provider ID',
            'Provider Postal Code': 'Provider Postal Code / Zip Code',
            'Provider Province/Territories': 'Provider Province/Territory',
            'CFR Date': 'Claim Funding Date',
            'Cheques': 'Cheque'
        }

        logger.debug("[DEBUG] Renaming columns based on map:", column_map_dict)

        self.df_raw.rename(columns=column_map_dict, inplace=True)

        logger.debug("[DEBUG] Data after renaming columns:")
        logger.debug(self.df_raw.head())


    def unpivot_dataframe(self):
        """
        Unpivot the DataFrame if lvl2_loc == 'FI02'.
        Returns original DataFrame unchanged otherwise.

        Unpivots a DataFrame by keeping specified columns and transforming the rest.
        
        Parameters:
        - df (pd.DataFrame): The input DataFrame.
        - columns_to_keep (list): List of columns to keep fixed (id_vars).
        - var_name (str): Name of the new column for former column headers.
        - value_name (str): Name of the new column for values.

        Returns:
        - pd.DataFrame: The unpivoted DataFrame.
        """
        columns_to_unpivot = ['Payment Cancellations Cheque',
                            'Payment Cancellations EFT',
                            'Refunds Cheque',
                            'Refunds EFT',
                            'Voids Cheque',
                            'Voids EFT',
                            'Cheque',
                            'EFT',
                            'Other Adjustments']

        logger.debug("[DEBUG] Columns to unpivot:", columns_to_unpivot)

        # Check which columns in columns_to_unpivot are present in the DataFrame
        existing_columns_to_unpivot = [col for col in columns_to_unpivot if col in self.df_raw.columns]
        logger.debug(f"[DEBUG] Existing columns to unpivot in DataFrame: {existing_columns_to_unpivot}")

        # If none of the columns to unpivot are present, skip the unpivot
        if set(columns_to_unpivot) != set(existing_columns_to_unpivot):
            logger.debug("[DEBUG] Some columns are missing, skipping unpivot.")
            return self.df_raw
        else:
            columns_to_keep = [col for col in self.df_raw.columns if col not in columns_to_unpivot]
            logger.debug(f"[DEBUG] Columns to keep: {columns_to_keep}")

            # Unpivot the DataFrame
            unpivoted_df = pd.melt(
                self.df_raw,
                id_vars=columns_to_keep,
                var_name="Payment Method",
                value_name="Paid Amount"
            )

            # Filter out rows where Paid Amount is zero
            logger.debug("[DEBUG] Unpivoted DataFrame (before filtering):")

            self.df_raw = unpivoted_df[unpivoted_df["Paid Amount"] != 0].reset_index(drop=True)

            logger.debug("[DEBUG] Data after unpivoting and filtering Paid Amount == 0:")

    def pad_leading_zeros(self):
        """Pads leading zeros for specified columns with different lengths, excluding NaNs and cleaning '.0' suffixes."""
        logger.debug("[DEBUG] Starting: pad_leading_zeros")

        column_pad_map = {
            'Procedure Code Paid': 5,
            'Procedure Code Submitted': 5,
            'Tooth Number': 2,
            'Provider Facility ID': 9,
            'Provider ID': 9,
            'Member Postal Code / Zip Code': 5
        }

        for column, length in column_pad_map.items():
            if column in self.df_raw.columns:
                logger.debug(f"[DEBUG] Processing column: {column} → pad to length {length}")

                # Convert to string and remove ".0" if present
                self.df_raw[column] = self.df_raw[column].astype(str).str.replace(r'\.0$', '', regex=True)

                # Replace 'nan' strings back to blank
                self.df_raw[column] = self.df_raw[column].replace('nan', '')

                # Apply padding only to non-null values
                self.df_raw[column] = self.df_raw[column].where(
                    self.df_raw[column].isna(), 
                    self.df_raw[column].str.zfill(length)
                )

                # Debug output
                logger.debug(f"[DEBUG] Sample values from '{column}':")
                logger.debug(self.df_raw[column].dropna().unique()[:5])
            else:
                logger.debug(f"[DEBUG] Column '{column}' not found in DataFrame")

        logger.debug("[DEBUG] Completed: pad_leading_zeros")

    
    def clean_mappings(self):
        """Detect and map province and specialty columns automatically."""
        # Province Mapping
        province_mapping = {
            'ON': 'Ontario', 'QC': 'Quebec', 'BC': 'British Columbia',
            'AB': 'Alberta', 'MB': 'Manitoba', 'SK': 'Saskatchewan',
            'NS': 'Nova Scotia', 'NB': 'New Brunswick', 'PE': 'Prince Edward Island',
            'NL': 'Newfoundland and Labrador', 'YT': 'Yukon',
            'NT': 'Northwest Territories', 'NU': 'Nunavut'
        }
        full_province_set = set(province_mapping.values())

        # Specialty Mapping
        specialty_mapping = {
            'GP' : 'General practitioner' ,
            'HY' : 'Dental hygienist' ,
            'DT' : 'Denturist' ,
            'EN' : 'Endodontist' ,
            'OP' : 'Oral pathologist' ,
            'OR' : 'Oral radiologist' ,
            'OS' : 'Oral and maxillofacial surgeon' ,
            'OT' : 'Orthodontist' ,
            'PD' : 'Pediatric dentist' ,
            'PE' : 'Periodontist' ,
            'PR' : 'Prosthodontist' ,
            'OM' : 'Oral medicine' ,
            'AN' : 'Anaesthesiologist' ,
            'DS' : 'Dental school' ,
            'DENOFF' : 'Dental Office',
            'MGP' : 'Medical General Practionner'
        }
        full_specialty_set = set(specialty_mapping.values())

        # Procedure Code Mapping
        procedure_code_mapping = {
            '00115': 'Diagnostic',
            '29201': 'Restorative',
            '39212': 'Endodontics'
        }

        # Identify relevant columns
        logger.debug("[DEBUG] Identifying relevant columns...")

        specialty_cols = [col for col in self.df_raw.columns if 'specialty' in col.lower()]
        logger.debug(f"[DEBUG] Specialty columns found: {specialty_cols}")

        province_cols = [col for col in self.df_raw.columns if 'province' in col.lower()]
        logger.debug(f"[DEBUG] Province columns found: {province_cols}")

        procedure_code_cols = [col for col in self.df_raw.columns if 'Procedure Code Paid' in col.title()]
        logger.debug(f"[DEBUG] Procedure Code columns found: {procedure_code_cols}")

        # Apply province mapping
        logger.debug("[DEBUG] Applying province mapping...")

        for col in province_cols:
            logger.debug(f"[DEBUG] Processing column: {col} for province mapping")
            
            before_change = self.df_raw[col].head()  # Show the first few values before change
            logger.debug(f"[DEBUG] Before province mapping: {before_change}")

            self.df_raw[col] = self.df_raw[col].apply(
                lambda x: province_mapping.get(x) 
                        if x in province_mapping 
                        else (x if x in full_province_set else "Unknown")
            )

            after_change = self.df_raw[col].head()  # Show the first few values after change
            logger.debug(f"[DEBUG] After province mapping: {after_change}")

        # Apply specialty mapping
        logger.debug("[DEBUG] Applying specialty mapping...")

        for col in specialty_cols:
            logger.debug(f"[DEBUG] Processing column: {col} for specialty mapping")

            before_change = self.df_raw[col].head()  # Show before change
            logger.debug(f"[DEBUG] Before specialty mapping: {before_change}")

            self.df_raw[col] = self.df_raw[col].astype(str).apply(
                lambda x: specialty_mapping.get(x.strip(), x.strip()) if x.strip().lower() in 
                {key.lower() for key in specialty_mapping.keys()} else x
            )

            after_change = self.df_raw[col].head()  # Show after change
            logger.debug(f"[DEBUG] After specialty mapping: {after_change}")

        # Apply procedure code mapping
        logger.debug("[DEBUG] Applying procedure code mapping...")

        for col in procedure_code_cols:
            logger.debug(f"[DEBUG] Processing column: {col} for procedure code mapping")

            before_change = self.df_raw['Benefit Category'].head()  # Show before change
            logger.debug(f"[DEBUG] Before procedure code mapping: {before_change}")

            self.df_raw['Benefit Category'] = self.df_raw.apply(
                lambda row: procedure_code_mapping.get(row[col].strip(), row['Benefit Category'])  
                if pd.notna(row[col]) and row[col].strip() in procedure_code_mapping  
                else row['Benefit Category'],  
                axis=1
            )

            after_change = self.df_raw['Benefit Category'].head()  # Show after change
            logger.debug(f"[DEBUG] After procedure code mapping: {after_change}")

        logger.debug("[DEBUG] Mapping process completed.")

    def clean_column_type(self):
        logger.debug("[DEBUG] Starting: clean_column_type")
        
        for column in self.df_raw.columns:
            col_lower = column.lower()
            logger.debug(f"[DEBUG] Processing column: {column} (dtype: {self.df_raw[column].dtype})")

            if 'date' in col_lower:
                self.df_raw[column] = pd.to_datetime(self.df_raw[column], errors="coerce")
                logger.debug(f"[DEBUG] Converted to datetime: {column}")
            
            elif 'id' in column.lower():
                self.df_raw[column] = self.df_raw[column].astype(str).str.replace(r'\.0$', '', regex=True)
                logger.debug(f"[DEBUG] Converted to string and removed trailing '.0': {column}")
            
            elif 'age' in col_lower and self.df_raw[column].dtype == 'object':
                self.df_raw[column] = pd.to_numeric(self.df_raw[column], errors='coerce').fillna(0).astype('int64')
                logger.debug(f"[DEBUG] Converted to numeric (int): {column}")
            
            elif 'amount' in col_lower and self.df_raw[column].dtype == 'object':
                self.df_raw[column] = self.df_raw[column].str.replace(',', '').astype(float)
                logger.debug(f"[DEBUG] Cleaned and converted to float: {column}")

        logger.debug("[DEBUG] Completed: clean_column_type")


    def filter_data(self):
        logger.debug("[DEBUG] Starting: filter_data")
        
        if "Cheque Number" in self.df_raw.columns:
            initial_count = len(self.df_raw)
            self.df_raw = self.df_raw[self.df_raw["Cheque Number"] != "WRITEOFF"].reset_index(drop=True)
            filtered_count = len(self.df_raw)
            logger.debug(f"[DEBUG] Filtered 'WRITEOFF' cheques: {initial_count - filtered_count} rows removed")

        if "Claim Reference Number" in self.df_raw.columns:
            initial_count = len(self.df_raw)
            self.df_raw = self.df_raw[self.df_raw["Claim Reference Number"] != "000000-00000-00"].reset_index(drop=True)
            filtered_count = len(self.df_raw)
            logger.debug(f"[DEBUG] Filtered zero reference: {initial_count - filtered_count} rows removed")

        logger.debug("[DEBUG] Completed: filter_data")

    def drop_column(self):
        if "Member Age" in self.df_raw.columns:
            self.df_raw = self.df_raw.drop(columns=["Member Age"])
        
        if "Benefit Category" in self.df_raw.columns:
            self.df_raw = self.df_raw.drop(columns=["Benefit Category"])

    def cheque_reformat(self):
        if "Cheque Number" in self.df_raw.columns:
            self.df_raw["Cheque Number"] = (
                self.df_raw["Cheque Number"]
                .astype(str)
                .str.replace(r"\.0$", "", regex=True)
            ) 

    def process_columns(self):
        logger.debug("[DEBUG] Starting process_columns()")

        self.remove_repeated_headers()
        logger.debug("[DEBUG] Completed: remove_repeated_headers")

        self.clean_column_name()
        logger.debug("[DEBUG] Completed: clean_column_name")

        self.unpivot_dataframe()
        logger.debug("[DEBUG] Completed: unpivot_dataframe")

        self.pad_leading_zeros()
        logger.debug("[DEBUG] Completed: pad_leading_zeros")

        self.clean_mappings()
        logger.debug("[DEBUG] Completed: clean_mappings")

        self.clean_column_type()
        logger.debug("[DEBUG] Completed: clean_column_type")

        self.filter_data()
        logger.debug("[DEBUG] Completed: filter_data")

        self.drop_column()
        logger.debug("[DEBUG] Completed: drop_column")

        self.cheque_reformat()
        logger.debug("[DEBUG] Completed: cheque_reformat")

        logger.debug("[DEBUG] Finished process_columns()")

        return self.df_raw  # Return processed DataFrame

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DataFrame Profiler

# CELL ********************

class DataProfiler:
    """
    A class to analyze each column in a DataFrame, collecting statistics such as data type, 
    unique values, null counts, min/max values, and string lengths.
    """

    def __init__(self, df):
        self.df = df.copy()  # Copy to avoid modifying the original DataFrame
        self.process_columns()
        self.arrow_table = pa.Table.from_pandas(self.df)
        self.profile = self.generate_profile()

    def process_columns(self):
        """Preprocess the DataFrame by converting date and ID columns."""
        for column in self.df.columns:
            if 'date' in column.lower():  
                self.df[column] = pd.to_datetime(self.df[column], errors='coerce')
            elif 'ID' in column:
                self.df[column] = self.df[column].astype(str)

    def generate_profile(self):
        """Generate a statistical profile of the DataFrame."""
        stats_data = []

        for i in range(len(self.arrow_table.schema)):
            column_name = self.arrow_table.schema[i].name
            column_type = str(self.arrow_table.schema[i].type)
            column_data = self.arrow_table[column_name].to_pandas()

            total_records = len(column_data)
            unique_values = column_data.nunique()
            null_count = column_data.isnull().sum()
            has_nulls = null_count > 0
            all_nulls = null_count == total_records

            min_value, max_value, min_len, max_len = None, None, None, None

            if column_type in ['int64', 'float64', 'double']:
                min_value = int(column_data.min())
                max_value = int(column_data.max())
                min_len = int(len(str(min_value))) if pd.notna(min_value) else None
                max_len = int(len(str(max_value))) if pd.notna(max_value) else None

            elif column_type == 'string':
                non_null_values = column_data.dropna().sort_values()
                if not non_null_values.empty:
                    min_value = non_null_values.iloc[0]
                    max_value = non_null_values.iloc[-1]
                lengths = column_data.dropna().astype(str).apply(len)
                min_len = int(lengths.min()) if not lengths.empty else None
                max_len = int(lengths.max()) if not lengths.empty else None

            elif column_type == 'bool':
                min_value = column_data.min()
                max_value = column_data.max()
                min_len = int(len(str(min_value))) if pd.notna(min_value) else None
                max_len = int(len(str(max_value))) if pd.notna(max_value) else None

            elif column_type == 'timestamp[ns]':
                min_value = column_data.min()
                max_value = column_data.max()
                min_len = int(len(str(min_value))) if pd.notna(min_value) else None
                max_len = int(len(str(max_value))) if pd.notna(max_value) else None

            stats_data.append({
                'Column Name': column_name,
                'Field Type': column_type,
                'Total Records': total_records,
                'Unique Values': unique_values,
                'Has Nulls': has_nulls,
                'All Nulls': all_nulls,
                'Min Value': min_value,
                'Max Value': max_value,
                'Min Length': min_len,
                'Max Length': max_len
            })

        return pd.DataFrame(stats_data)

    def get_profile(self):
        """Return the DataFrame profile."""
        return self.profile

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DataFrame Comparer

# CELL ********************

class DataFrameComparer:
    def __init__(self, df1, df2):
        self.df1 = df1
        self.df2 = df2
        self.report = {"Metric": [], "Result": []}
        self.differences = {}
        self.comparison = {}

    def log_difference(self, key, value):
        """Logs only if there's a difference."""
        if isinstance(value, pd.DataFrame) and not value.empty:
            self.differences[key] = value
        elif isinstance(value, dict) and value:  # Check if dict is not empty
            self.differences[key] = value
        elif isinstance(value, tuple) and value[0] != value[1]:  # Check tuple differences
            self.differences[key] = value
        elif isinstance(value, str):  # Log errors as strings
            self.differences[key] = value

    def log_comparison(self, key, value):
        """Logs all the comparison values."""
        if isinstance(value, pd.DataFrame) and not value.empty:
            self.comparison[key] = value
        elif isinstance(value, dict) and value:  # Check if dict is not empty
            self.comparison[key] = value
        elif isinstance(value, tuple):  # Check tuple differences
            self.comparison[key] = value
        elif isinstance(value, str):  # Log errors as strings
            self.comparison[key] = value

    def compare_row_count(self):
        try:
            row_count = (self.df1.shape[0], self.df2.shape[0])

            row_count_comparison = pd.DataFrame([row_count], columns=['Dataset 1', 'Dataset 2'])

            self.log_comparison("Row Count Comparison", row_count_comparison)

            if row_count[0] != row_count[1]:
                self.log_difference("Row Count Difference", row_count)
                self.report["Metric"].append("Row Count Difference")
                self.report["Result"].append("Failed")
            else:
                self.report["Metric"].append("Row Count Difference")
                self.report["Result"].append("Passed")
        except Exception as e:
            self.log_difference("Row Count Error", str(e))
            self.report["Metric"].append("Row Count Error")
            self.report["Result"].append("Error")

    def compare_column_count(self):
        try:
            col_count = (self.df1.shape[1], self.df2.shape[1])

            col_count_comparison = pd.DataFrame([col_count], columns=['Dataset 1', 'Dataset 2'])

            self.log_comparison("Column Count Comparison", col_count_comparison)

            if col_count[0] != col_count[1]:
                self.log_difference("Column Count Difference", col_count)
                self.report["Metric"].append("Column Count Difference")
                self.report["Result"].append("Failed")
            else:
                self.report["Metric"].append("Column Count Difference")
                self.report["Result"].append("Passed")
        except Exception as e:
            self.log_difference("Column Count Error", str(e))
            self.report["Metric"].append("Column Count Error")
            self.report["Result"].append("Error")

    def compare_column_names(self):
        try:
            """
            Generates a side-by-side comparison of column names between two DataFrames (self.df1 and self.df2).

            Steps:
            - Combines and sorts all unique column names from both DataFrames.
            - Creates a DataFrame showing:
                - The presence of each column in Dataset 1 and Dataset 2.
                - A 'Match' status indicating whether the column exists in both DataFrames ('Pass') or not ('Fail').
            - Logs the comparison using `self.log_comparison` with the title "Column Names Comparison".

            Used for a visual audit of schema alignment between datasets.
            """

            all_columns = sorted(self.df1.columns.union(self.df2.columns))

            col_name_comparison = pd.DataFrame({
                'Column Name': all_columns,
                'Dataset 1 Columns': ['x' if col in self.df1.columns else '' for col in all_columns],
                'Dataset 2 Columns': ['x' if col in self.df2.columns else '' for col in all_columns],
                'Match': ['Pass' if col in self.df1.columns and col in self.df2.columns else 'Fail' for col in all_columns]
            })

            self.log_comparison("Column Names Comparison", col_name_comparison)

            """
            Compares column names between two DataFrames (self.df1 and self.df2) and logs any differences.

            Steps:
            - Identifies common columns shared between both DataFrames.
            - Strips whitespace and compares the column names from each DataFrame.
            - If differences are found, logs the column names that are unique to each DataFrame.
            - Appends the result ("Passed" or "Failed") to the self.report dictionary under the "Column Names Difference" metric.

            Used for validating schema consistency between datasets.
            """
            common_columns = self.df1.columns.intersection(self.df2.columns)
            df1_cols = set(col.strip() for col in self.df1[common_columns])
            df2_cols = set(col.strip() for col in self.df2[common_columns])
            col_names_match = df1_cols == df2_cols

            if not col_names_match:
                self.log_difference("Column Names Difference", {
                    "Only in Dataset 1": list(set(self.df1.columns) - set(self.df2.columns)),
                    "Only in Dataset 2": list(set(self.df2.columns) - set(self.df1.columns))
                })
                self.report["Metric"].append("Column Names Difference")
                self.report["Result"].append("Failed")
            else:
                self.report["Metric"].append("Column Names Difference")
                self.report["Result"].append("Passed")
        except Exception as e:
            self.log_difference("Column Names Error", str(e))
            self.report["Metric"].append("Column Names Error")
            self.report["Result"].append("Error")

    def compare_missing_values(self):
        try:
            """
            Generates a detailed comparison of missing values between `self.df1` and `self.df2` across all columns.

            Steps:
            - Calculates the percentage of missing values for each column in both datasets.
            - Merges the column sets from both datasets to ensure a complete comparison.
            - For each column:
                - Computes the missing and non-missing record counts in both datasets.
                - Calculates the percentage of missing data.
                - Flags each column as "Pass" if the missing percentages are equal in both datasets, otherwise "Fail".

            Creates a summary DataFrame (`missing_diff_comparison`) with the following columns:
            - 'Column Name'
            - 'Dataset 1 Missing Count'
            - 'Dataset 1 Record Count'
            - 'Dataset 2 Missing Count'
            - 'Dataset 2 Record Count'
            - 'Dataset 1 Missing %'
            - 'Dataset 2 Missing %'
            - 'Match'

            Returns:
            - `missing_diff_comparison` (pd.DataFrame): A comparison table for missing values and match status across datasets.
            """
            missing_comp_df1 = (self.df1.isna().sum() / len(self.df1)) * 100
            missing_comp_df2 = (self.df2.isna().sum() / len(self.df2)) * 100

            df1_cols = set(self.df1.columns)
            df2_cols = set(self.df2.columns)
            all_columns = sorted(df1_cols.union(df2_cols))

            missing_diff_comparison = pd.DataFrame({
                'Column Name': all_columns,
                'Dataset 1 Missing Count': [int(self.df1[col].isna().sum()) if col in df1_cols else '' for col in all_columns],
                'Dataset 1 Record Count': [int(self.df1[col].count()) if col in df1_cols else '' for col in all_columns],
                'Dataset 2 Missing Count': [int(self.df2[col].isna().sum()) if col in df2_cols else '' for col in all_columns],
                'Dataset 2 Record Count': [int(self.df2[col].count()) if col in df2_cols else '' for col in all_columns],
                'Dataset 1 Missing %': [f'{round(missing_comp_df1[col], 3)}%' if col in df1_cols else '' for col in all_columns],
                'Dataset 2 Missing %': [f'{round(missing_comp_df2[col], 3)}%' if col in df2_cols else '' for col in all_columns],
                'Match': ['Pass' if col in missing_comp_df1 
                                    and col in missing_comp_df2 
                                    and missing_comp_df1[col] == missing_comp_df2[col] 
                                    else 'Fail' for col in all_columns]
                    })

            self.log_comparison("Missing Values Comparison", missing_diff_comparison)

            """
            Compares the percentage of missing values for each common column between two DataFrames (`self.df1` and `self.df2`).

            Process:
            - Identifies the common columns shared by both DataFrames.
            - Computes the percentage of missing (NaN) values for each common column in both DataFrames.
            - Compares the missing value percentages using `.compare()` to identify differences.
            - If differences are found, logs them and records the result as "Failed".
            - If no differences are found, records the result as "Passed".

            Used to validate consistency in missing data patterns across datasets.

            Side Effects:
            - Appends to `self.report["Metric"]` and `self.report["Result"]`.
            - Logs differences using `self.log_difference()` if mismatches exist.

            Returns:
            - None. Updates internal state (`self.report`) and logs as needed.
            """

            common_columns = self.df1.columns.intersection(self.df2.columns)
            df1_sorted = self.df1[common_columns]
            df2_sorted = self.df2[common_columns]

            missing_values_df1 = (df1_sorted.isna().sum() / len(df1_sorted)) * 100
            missing_values_df2 = (df2_sorted.isna().sum() / len(df2_sorted)) * 100

            missing_diff = missing_values_df1.compare(missing_values_df2, result_names=('Dataset 1', 'Dataset 2'))

            if not missing_diff.empty:
                self.log_difference("Missing Values Difference", missing_diff)
                self.report["Metric"].append("Missing Values Difference")
                self.report["Result"].append("Failed")
            else:
                self.report["Metric"].append("Missing Values Difference")
                self.report["Result"].append("Passed")
        except Exception as e:
            self.log_difference("Missing Values Error", str(e))
            self.report["Metric"].append("Missing Values Error")
            self.report["Result"].append("Error")

    def compare_duplicates(self):
        try:
            duplicate_rows = (self.df1.duplicated().sum(), self.df2.duplicated().sum())

            #  Detailed view of the duplicated records
            duplicates_dict = {
                "Dataset 1 Duplicate Records": self.df1[self.df1.duplicated(keep=False)],
                "Dataset 2 Duplicate Records": self.df2[self.df2.duplicated(keep=False)]
            }

            self.log_comparison("Duplicate Records", duplicates_dict)

            if duplicate_rows[0] != duplicate_rows[1]:
                self.log_difference("Duplicate Rows Difference", duplicate_rows)
                self.report["Metric"].append("Duplicate Rows Count Difference")
                self.report["Result"].append("Failed")
            else:
                self.report["Metric"].append("Duplicate Rows Count Difference")
                self.report["Result"].append("Passed")
        except Exception as e:
            self.log_difference("Duplicate Rows Error", str(e))
            self.report["Metric"].append("Duplicate Rows Error")
            self.report["Result"].append("Error")

    def compare_unique_values(self):
        try:
            unique_comp_df1 = self.df1.nunique()
            unique_comp_df2 = self.df2.nunique()
            
            df1_cols = set(self.df1.columns)
            df2_cols = set(self.df2.columns)
            all_columns = sorted(df1_cols.union(df2_cols))

            #  Detailed view of the unique value comparison
            unique_value_comparison = pd.DataFrame({
                'Column Name': all_columns,
                'Dataset 1 Unique Record Count': [
                    unique_comp_df1[col] if col in df1_cols else '' for col in all_columns
                ],
                'Dataset 2 Unique Record Count': [
                    unique_comp_df2[col] if col in df2_cols else '' for col in all_columns
                ],
                'Match': [
                    'Pass' if col in unique_comp_df1 
                            and col in unique_comp_df2 
                            and unique_comp_df1[col] == unique_comp_df2[col] else 'Fail' for col in all_columns
                ],
                'Unique Values Dataset 1': [
                    sorted([str(x) for x in self.df1[col].unique()]) if col in df1_cols 
                                                                    and unique_comp_df1[col] <= 100 
                                                                    and 'date' not in col.lower() 
                    else '' 
                    for col in all_columns
                ],
                'Unique Values Dataset 2': [
                    sorted([str(x) for x in self.df2[col].unique()]) if col in df2_cols 
                                                                    and unique_comp_df2[col] <= 100 
                                                                    and 'date' not in col.lower() 
                    else '' 
                    for col in all_columns
                ]
            })

            self.log_comparison("Unique Values Comparison", unique_value_comparison)

            common_columns = self.df1.columns.intersection(self.df2.columns)
            df1_sorted = self.df1[common_columns]
            df2_sorted = self.df2[common_columns]

            unique_values_df1 = df1_sorted.nunique()
            unique_values_df2 = df2_sorted.nunique()

            unique_values_diff = unique_values_df1.compare(unique_values_df2, result_names=('Dataset 1', 'Dataset 2'))

            if not unique_values_diff.empty:
                self.log_difference("Unique Values Difference", unique_values_diff)
                self.report["Metric"].append("Unique Values Difference")
                self.report["Result"].append("Failed")
            else:
                self.report["Metric"].append("Unique Values Difference")
                self.report["Result"].append("Passed")
        except Exception as e:
            self.log_difference("Unique Values Error", str(e))
            self.report["Metric"].append("Unique Values Error")
            self.report["Result"].append("Error")

    def compare_statistics(self):
        try:
            #  Detailed view of the descriptive stats
            numerical_stats = {
                "Dataset 1 Stat Description": self.df1.describe().round(2),
                "Dataset 2 Stat Description": self.df2.describe().round(2)
            }

            self.log_comparison("Statistical Summary Comparison", numerical_stats)

            common_columns = self.df1.columns.intersection(self.df2.columns)
            df1_sorted = self.df1[common_columns]
            df2_sorted = self.df2[common_columns]

            numerical_cols = df1_sorted.select_dtypes(include=np.number).columns
            df1_stats = df1_sorted[numerical_cols].describe().round(2)
            df2_stats = df2_sorted[numerical_cols].describe().round(2)

            stats_diff = df1_stats.compare(df2_stats, result_names=('Dataset 1', 'Dataset 2'))

            if not stats_diff.empty:
                self.log_difference("Statistical Summary Difference", stats_diff)
                self.report["Metric"].append("Statistical Summary Difference")
                self.report["Result"].append("Failed")
            else:
                self.report["Metric"].append("Statistical Summary Difference")
                self.report["Result"].append("Passed")
        except Exception as e:
            self.log_difference("Statistical Summary Error", str(e))
            self.report["Metric"].append("Statistical Summary Error")
            self.report["Result"].append("Error")

    def compare_row_wise(self):
        try:
            common_columns = self.df1.columns.intersection(self.df2.columns)
            #Ignore Claim Funding Date Difference
            common_columns = [col for col in common_columns if col != "Claim Funding Date"]

            df1_sorted = self.df1[common_columns]
            df2_sorted = self.df2[common_columns]

            # Sort columns to ensure they have the same order
            df1_sorted = df1_sorted[sorted(df1_sorted.columns)]
            df2_sorted = df2_sorted[sorted(df2_sorted.columns)]

            # Sort values by ALL columns to ensure row order matches
            df1_sorted = df1_sorted.sort_values(by=df1_sorted.columns.tolist()).reset_index(drop=True)
            df2_sorted = df2_sorted.sort_values(by=df2_sorted.columns.tolist()).reset_index(drop=True)

            df2_sorted = df2_sorted.reindex(df1_sorted.index)

            row_differences = df1_sorted.compare(df2_sorted, result_names=('Dataset 1', 'Dataset 2'))

            if not row_differences.empty:
                differing_indices = row_differences.index  # Get indices where differences exist
                
                # Retrieve full records from the original DataFrames
                differing_records_df1 = df1_sorted.loc[differing_indices]
                differing_records_df2 = df2_sorted.loc[differing_indices]

                self.log_difference("Full Row Differences - Dataset 1", differing_records_df1)
                self.log_difference("Full Row Differences - Dataset 2", differing_records_df2)
                self.log_difference("Row-wise Differences Summary", row_differences)

                self.report["Metric"].append("Row-wise Differences")
                self.report["Result"].append("Failed")
            else:
                self.report["Metric"].append("Row-wise Differences")
                self.report["Result"].append("Passed")
        except Exception as e:
            self.log_difference("Row-wise Comparison Error", str(e))
            self.report["Metric"].append("Row-wise Comparison Error")
            self.report["Result"].append("Error")

    def compare_categorical_distributions(self):
        try:
            df1_categorical = self.df1.select_dtypes(include="object").columns
            df2_categorical = self.df2.select_dtypes(include="object").columns

            all_categorical_columns = sorted(set(df1_categorical).union(set(df2_categorical)))

            #  Detailed view of the categorical distributions
            dist_comparison = pd.DataFrame({
                                'Column Names': all_categorical_columns,
                                'Key Length DF1': [len(self.df1[col].value_counts().keys()) if col in df1_categorical else '' for col in all_categorical_columns],
                                'Key Length DF2': [len(self.df2[col].value_counts().keys()) if col in df2_categorical else '' for col in all_categorical_columns],
                                'Match': ['Pass' if col in df1_categorical and col in df2_categorical and 
                                                len(self.df1[col].value_counts()) == len(self.df2[col].value_counts()) 
                                                else 'Fail' for col in all_categorical_columns]
                                                })
            categorical_dist = {
                                'Distribution Comparison': dist_comparison,
                                'Categorical Distribution Dataset 1': {
                                                        col: self.df1[col].value_counts()
                                                        for col in all_categorical_columns if col in df1_categorical and len(self.df1[col].value_counts().keys()) < 100
                                                    },
                                'Categorical Distribution Dataset 2': {
                                                        col: self.df2[col].value_counts()
                                                        for col in all_categorical_columns if col in df2_categorical and len(self.df2[col].value_counts().keys()) < 100
                                                    }
                            }

            self.log_comparison("Categorical Distribution", categorical_dist)


            # Step 1: Get categorical columns from df1
            df1_object_type = self.df1.select_dtypes(include="object").columns

            # Step 2: Find intersection with df2's columns
            categorical_cols = df1_object_type.intersection(self.df2.columns)

            df1_sorted = self.df1[categorical_cols]
            df2_sorted = self.df2[categorical_cols]
            
            # Sort columns to ensure they have the same order
            df1_sorted = df1_sorted[sorted(df1_sorted.columns)]
            df2_sorted = df2_sorted[sorted(df2_sorted.columns)]

            # Sort values by ALL columns to ensure row order matches
            df1_sorted = df1_sorted.sort_values(by=df1_sorted.columns.tolist()).reset_index(drop=True)
            df2_sorted = df2_sorted.sort_values(by=df2_sorted.columns.tolist()).reset_index(drop=True)

            df2_sorted = df2_sorted.reindex(df1_sorted.index)

            cat_diff = {
                col: df1_sorted[col].value_counts().compare(df2_sorted[col].value_counts(), result_names=('Dataset 1', 'Dataset 2')).dropna()
                for col in categorical_cols if not df1_sorted[col].value_counts().compare(df2_sorted[col].value_counts(), result_names=('Dataset 1', 'Dataset 2')).empty
            }

            if cat_diff:
                self.log_difference("Categorical Distributions Difference", cat_diff)
                self.report["Metric"].append("Categorical Distributions Difference")
                self.report["Result"].append("Failed")
            else:
                self.report["Metric"].append("Categorical Distributions Difference")
                self.report["Result"].append("Passed")
        except Exception as e:
            self.log_difference("Categorical Distributions Error", str(e))
            self.report["Metric"].append("Categorical Distributions Error")
            self.report["Result"].append("Error")

    def compare(self):
        self.compare_row_count()
        self.compare_column_count()
        self.compare_column_names()
        self.compare_missing_values()
        self.compare_unique_values()
        self.compare_duplicates()
        self.compare_row_wise()
        self.compare_statistics()
        self.compare_categorical_distributions()

        # Create a DataFrame for the metrics report
        metrics = pd.DataFrame(self.report)
        
        return {"metrics": metrics, "differences": self.differences, "comparison": self.comparison}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Report Generator

# CELL ********************

class ReportGenerator:
    def __init__(self, result):
        """
        Initializes the ReportGenerator with result dictionary.
        
        Args:
        - result (dict): The global dictionary containing report data.
        """
        self.result = result
        self.metrics_result = result.get('metrics', {})
        self.comparison_result = result.get('comparison', {})
        self.differences_result = result.get('differences', {})
        self_reports = self.generate_report()

    def generate_report(self):
        """
        Generates different reports based on the type and content of metric names.

        Returns:
        - dict: A dictionary containing different reports as DataFrames.
        """

        error_list = []
        failed_list = []
        count_report_list = []
        dict_report_list = {}
        df_report_dict = {}
        for metric_name, value in self.differences_result.items():
            # Condition: Error Message Handling
            if "Error" in metric_name:
                error_list.append({
                    'Metric Name': metric_name,
                    'Error Message': value
                })
            
            # Condition: Failed Metrics
            if "Difference" in metric_name:
                failed_list.append({
                    'Metric Name': metric_name,
                    'Failure Reason': value
                })

            # Condition: Count-based metrics with tuple values
            if isinstance(value, tuple):
                count_report_list.append({
                    'Metric Name': metric_name,
                    'Dataset 1': value[0],
                    'Dataset 2': value[1],
                })

            # Condition: Handling dictionary values. Creating dataframe for each report.
            elif isinstance(value, dict):
                for key in value.keys():
                    dataframe_name = metric_name + " - " + key
                    dict_report_list[dataframe_name] = pd.DataFrame(value[key], columns=[dataframe_name])

            # Condition: Handling DataFrame values. Storing dataframe for each dataframe.
            elif isinstance(value, pd.DataFrame):
                df_report_dict[metric_name] = value

        # Convert lists to DataFrames
        error_report_df = pd.DataFrame(error_list) if error_list else None
        failed_report_df = pd.DataFrame(failed_list) if failed_list else None
        count_report_df = pd.DataFrame(count_report_list) if count_report_list else None

        # Construct final output
        reports = {
            'error_report': error_report_df,
            'failed_report': failed_report_df,
            'count_report': count_report_df,
            'dict_report': dict_report_list,
            'df_reports': df_report_dict
        }

        return reports
    
    def get_metrics_result(self):
        return self.metrics_result

    def get_comparison_result(self):
        return self.comparison_result


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Main Functions

# MARKDOWN ********************

# ### Date Generator

# CELL ********************

from datetime import datetime, timedelta

class DateGenerator:
    def __init__(self, start_date, end_date, frequency='weekly'):
        """
        Initialize the DateGenerator.

        Parameters:
        - start_date (datetime): The starting date.
        - end_date (datetime): The ending date.
        - frequency (str): 'daily' or 'weekly'. If 'weekly', it returns Saturdays.
        """
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency.lower()
        
        if self.frequency not in ['daily', 'weekly']:
            raise ValueError("Frequency must be either 'daily' or 'weekly'")

    def generate_dates(self):
        """
        Generate dates between start_date and end_date according to the frequency.

        Returns:
        - List of date strings in 'YYYY-MM-DD' format.
        """
        dates = []
        current = self.start_date

        if self.frequency == 'weekly':
            # Move to the first Saturday
            current += timedelta(days=(5 - current.weekday()) % 7)
            while current <= self.end_date:
                dates.append(current.strftime('%Y-%m-%d'))
                current += timedelta(days=7)

        else:  # daily
            while current <= self.end_date:
                if current.weekday() > 0 and current.weekday() <= 5:  # Monday = 0, Sunday = 6
                    dates.append(current.strftime('%Y-%m-%d'))
                current += timedelta(days=1)

        return dates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Lakehouse Table Generator

# CELL ********************

class LakehouseTableGenerator:
    def __init__(self, dates, src1_label = 'src1', src2_label = 'src2'):
        self.dates = dates
        self.src1_label = src1_label
        self.src2_label = src2_label
        self.result_lists = {
            'Row Count Comparison': [],
            'Column Count Comparison': [],
            'Column Names Comparison': [],
            'Missing Values Comparison': [],
            'Unique Values Comparison': [],
            'Duplicate Records': [],
            'Statistical Summary Comparison': [],
            'Categorical Distribution': []
        }
        self.metrics_result = []
        self.dataset_profile = []

    def run_comparisons(self):
        for date in self.dates:
            global src1_date, src2_date
            src1_date = date
            src2_date = date

            source1 = DatasetImporter(self.src1_label).import_dataset()
            source2 = DatasetImporter(self.src2_label).import_dataset()

            dataframe1 = DataFrameCleaning(source1[0]).process_columns()
            dataframe2 = DataFrameCleaning(source2[0]).process_columns()

            comparison_result = DataFrameComparer(dataframe1, dataframe2).compare()
            comparison_reports = comparison_result['comparison']
            dataset1_name = source1[1]
            dataset2_name = source2[1]

            # Add row to metrics summary
            result = comparison_result['metrics']
            result['Dataset Source 1'] = dataset1_name
            result['Dataset Source 2'] = dataset2_name
            self.metrics_result.append(result)

            # Add row to profile summary
            for frame, name in [(source1[0], source1[1]), (source2[0], source2[1])]:
                logger.debug("[DEBUG] Processing frame and name:")
                logger.debug("[DEBUG]       frame type:", type(frame))
                logger.debug("[DEBUG]       name:", name)

                try:
                    data_profile = DataProfiler(frame).get_profile()
                    logger.debug("[DEBUG] Data profile created successfully.")

                    data_profile['Dataset'] = name
                    logger.debug("[DEBUG] Dataset name added to profile.")

                    self.dataset_profile.append(data_profile)
                    logger.debug("[DEBUG] Data profile appended to summary.")
                except Exception as e:
                    logger.debug(f"ERROR: Failed to process dataset {name} due to: {e}")

            self._process_comparison_reports(comparison_reports, dataset1_name, dataset2_name)

    def _process_comparison_reports(self, comparison_reports, dataset1_name, dataset2_name):
        for key in self.result_lists:
            frame = comparison_reports.get(key)
            if isinstance(frame, (pd.DataFrame, pd.Series)):
                df = frame.copy()
                df['Dataset Source 1'] = dataset1_name
                df['Dataset Source 2'] = dataset2_name
                df['Metric'] = key
                self.result_lists[key].append(df)

            elif key == 'Statistical Summary Comparison' and isinstance(frame, dict):
                stat_keys = ['Dataset 1 Stat Description', 'Dataset 2 Stat Description']
                for stat_key in stat_keys:
                    if stat_key in frame and isinstance(frame[stat_key], pd.DataFrame):
                        stat_df = frame[stat_key]
                        stat_df['Column Name'] = stat_df.index
                        stat_df_transposed = stat_df.T.reset_index()
                        stat_df_transposed.rename(columns={'index': 'Statistic'}, inplace=True)
                        stat_df_transposed['Dataset'] = dataset1_name if stat_key == 'Dataset 1 Stat Description' else dataset2_name
                        stat_df_transposed['Metric'] = key + f' - {stat_key}'
                        self.result_lists[key].append(stat_df_transposed)

            elif key == 'Categorical Distribution' and isinstance(frame, dict):
                for cat_key in ['Categorical Distribution Dataset 1', 'Categorical Distribution Dataset 2']:
                    if cat_key in frame and isinstance(frame[cat_key], dict):
                        for col, val_counts in frame[cat_key].items():
                            cat_df = val_counts.reset_index()
                            cat_df.columns = ['Value Name', 'Distribution']
                            cat_df['Column Name'] = col
                            cat_df['Dataset'] = dataset1_name if cat_key.endswith("1") else dataset2_name
                            cat_df['Metric'] = key + f' - {cat_key}'
                            self.result_lists[key].append(cat_df)

            elif key == 'Duplicate Records' and isinstance(frame, dict):
                select_columns = ['Member ID', 'Family ID', 'Claim Reference Number', 'Cheque Number', 
                                'Claim Line Number', 'Payment Method', 'Submitted Amount', 'Paid Amount']
                
                for dup_key in ['Dataset 1 Duplicate Records', 'Dataset 2 Duplicate Records']:
                    dup_df = frame[dup_key].copy()
                    
                    # Ensure all required columns are present
                    for col in select_columns:
                        if col not in dup_df.columns:
                            dup_df[col] = None  # or use np.nan if using pandas
                    
                    # Keep only the selected columns
                    dup_df = dup_df[select_columns]
                    
                    dup_df['Dataset'] = dataset1_name if dup_key == 'Dataset 1 Duplicate Records' else dataset2_name
                    dup_df['Metric'] = dup_key
                    
                    self.result_lists[key].append(dup_df)

            elif frame is not None:
                print(f"Skipped key '{key}' - Invalid type: {type(frame)}")

    def get_combined_results(self):
        final_results = {
            key: pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            for key, frames in self.result_lists.items()
        }
        metrics_report = pd.concat(self.metrics_result, ignore_index=True) if self.metrics_result else pd.DataFrame()
        profile_report = pd.concat(self.dataset_profile, ignore_index=True) if self.dataset_profile else pd.DataFrame()
        
        return final_results, metrics_report, profile_report
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Delta Table Writer

# CELL ********************

class DeltaTableWriter:
    def __init__(self, spark):
        """
        Initialize the DeltaTableWriter with a Spark session.
        """
        self.spark = spark

    def clean_column_names(self, df):
        """
        Clean column names by removing spaces, special characters, and replacing them with underscores.
        """
        df.columns = [col.strip()
                        .replace(" ", "_")
                        .replace("(", "")
                        .replace(")", "")
                        .replace("-", "_")
                        .replace(",", "")
                        .replace("=", "")
                        for col in df.columns]
        return df


    def replace_empty_strings(self, df):
        """
        Replace empty strings with None (null) in the DataFrame.
        """
        return df.replace(r'^\s*$', '', regex=True).replace({np.nan: ''}).replace({pd.NaT: ''})
        
    def convert_pandas_to_spark(self, df):
        df = df.astype(str)
        
        # Convert the Pandas DataFrame to PySpark DataFrame
        spark_df = spark.createDataFrame(df)
        return spark_df

    def enrich_df_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich the DataFrame by extracting 'Data Source Date', 'Universe', 
        and renaming/truncating the 'Metric' column.
        """
        if 'Dataset_Source_2' in df.columns:
            source_col = 'Dataset_Source_2'
        elif 'Dataset' in df.columns:
            source_col = 'Dataset'
        else:
            print("No source column found.")
            return df

        def extract_info(val):
            if pd.isna(val):
                return pd.NA, pd.NA

            val = str(val)
            parts = val.split('_')

            if len(parts) >= 2 and re.match(r'^\d{4}-?\d{2}-?\d{2}$', parts[0]):
                date = parts[0]
                universe = parts[1].capitalize()
            elif val.endswith('.csv') and not re.match(r'^\d{4}', val):
                universe = val[:4]
                try:
                    date = re.search(r'_([^_]+)\.csv$', val).group(1)
                except:
                    date = pd.NA
            else:
                date = pd.NA
                universe = pd.NA
            return date, universe

        df[['Data_Source_Date', 'Universe']] = df[source_col].apply(
            lambda val: pd.Series(extract_info(val))
        )

        if 'Metric' in df.columns:
            df['Metric_Category'] = df['Metric'].str.split().apply(
                lambda x: ' '.join(x[:-1]) if len(x) > 1 else ''
            )
            df.drop(columns=['Metric'], inplace=True)

        try:
            df['Primary_Key'] = df.apply(
                lambda row: f"{row['Universe']}_{row['Data_Source_Date']}_{row['Column_Name']}" 
                            if 'Column_Name' in df.columns and pd.notna(row['Column_Name']) 
                            else f"{row['Universe']}_{row['Data_Source_Date']}_{row['Metric_Category']}",
                axis=1
            )
        except:
            pass
                
        return df

    def prepare_df_for_delta(self, df: pd.DataFrame, table_name: str) -> tuple:
        """
        Prepares a pandas DataFrame for writing to Delta by cleaning, enriching,
        converting to PySpark, and adding a validation timestamp.

        Returns:
            (sdf, sanitized_table_name): tuple of Spark DataFrame and table name
        """
        if df.empty:
            print("DataFrame is empty. Skipping write operation.")
            return None, None

        logger.debug("[DEBUG] Cleaning column names.")
        df = self.clean_column_names(df)

        logger.debug("[DEBUG] Enriching DataFrame.")
        df = self.enrich_df_report(df)

        logger.debug("[DEBUG] Replacing empty strings with NaN.")
        df = self.replace_empty_strings(df)

        logger.debug("[DEBUG] Creating a pyspark schema.")
        sdf = self.convert_pandas_to_spark(df)

        logger.debug("[DEBUG] Adding Validation Date column.")
        sdf = sdf.withColumn("Validation_Date", from_utc_timestamp(current_timestamp(), "Canada/Eastern"))

        logger.debug("[DEBUG] Sanitizing table name.")
        sanitized_table_name = table_name.strip().replace(" ", "_").lower()

        return sdf, sanitized_table_name

    def write_to_delta_table(self, df, table_name: str, write_mode: str = None):
        """
        Writes a Spark DataFrame to a Delta table, either appending or overwriting
        based on table existence or user-specified write_mode.
        """
        sdf, cleaned_table_name = self.prepare_df_for_delta(df, table_name)

        if sdf is None:
            logger.debug("[DEBUG] No data to write for table {cleaned_table_name}")
            return

        if write_mode is None:
            try:
                logger.debug("[DEBUG] Checking if Delta table '{cleaned_table_name}' exists.")
                delta_table = DeltaTable.forName(self.spark, cleaned_table_name)

                existing_columns = set([f.name for f in delta_table.toDF().schema.fields])
                incoming_columns = set(sdf.columns)

                logger.debug("[DEBUG] Existing columns in table '{cleaned_table_name}': {existing_columns}")
                logger.debug("[DEBUG] Incoming columns from DataFrame: {incoming_columns}")

                if existing_columns == incoming_columns:
                    write_mode = "append"
                    print("DEBUG: Existing table has matching schema. Using append mode.")
                else:
                    write_mode = "overwrite"
                    print("DEBUG: Existing table has different schema. Using overwrite mode.")
            except AnalysisException:
                logger.debug("[DEBUG] Table '{cleaned_table_name}' does not exist. Using overwrite mode.")
                write_mode = "overwrite"
        else:
            logger.debug("[DEBUG] Write mode explicitly set to '{write_mode}' by caller.")

        logger.debug("[DEBUG] Writing data to Delta table {cleaned_table_name}.")
        invalid_characters = r'[ ,;{}()\n\t=]'
        for column in sdf.columns:
            if re.search(invalid_characters, column):
                logger.debug("[DEBUG] Invalid column name: {column}")

        logger.debug("[DEBUG] Processing : {cleaned_table_name}")
        try:
            writer = sdf.write.format("delta").mode(write_mode)
            if write_mode == "overwrite":
                writer = writer.option("overwriteSchema", "true")

            writer.saveAsTable(cleaned_table_name)
            print(f"[DEBUG]  {write_mode.title()} operation successful for {cleaned_table_name}")
        except Exception as e:
            print(f"[DEBUG]  {write_mode.title()} operation failed for {cleaned_table_name}")
            raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dropping Selected Records Lakehouse

# CELL ********************

def remove_records_from_table(spark, table_name, universe_value, data_source_date_value):
    """
    Reads a Delta table from the Lakehouse, removes records based on Universe and Data_Source_Date,
    and overwrites the table with the filtered result.

    Parameters:
        spark: SparkSession
        table_name (str): Name of the Delta table
        universe_value (str): Value to filter 'Universe' column
        data_source_date_value (str): Value to filter 'Data_Source_Date' column
    """

    # Read the existing Delta table
    df = spark.read.format("delta").table(table_name)

    # Filter out rows that match BOTH conditions
    filtered_df = df.filter(
        ~((df["Universe"] == universe_value) & (df["Data_Source_Date"] == data_source_date_value))
    )

    # Overwrite the existing Delta table with the filtered data
    (
        filtered_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Logging

# CELL ********************

# Function to process logs and split the messages
def process_debug_logs():
    # Get the log output
    log_output = get_log_output()

    # Split the log output by line and filter out empty lines
    log_messages = log_output.split('\n')

    # Filter only messages that contain '[DEBUG]' after the 'DEBUG' level
    # Also ensure that we capture only the part up to '[DEBUG]'
    debug_messages = [
        message for message in log_messages if re.search(r' - DEBUG -.*\[DEBUG\](.*)', message)
    ]

    # Create a DataFrame with the filtered debug messages
    df_debug = pd.DataFrame(debug_messages, columns=['Log Message'])

    # Split the Log Message into Execution Timestamp, Info Type, and Debug Message
    df_split = df_debug['Log Message'].str.split(' - ', n=2, expand=True)
    # If there are fewer than 3 parts in some rows, fill the missing parts with None or a placeholder
    df_split = df_split.reindex(columns=[0, 1, 2, 3], fill_value=None)

    # Assign split columns to the new DataFrame
    df_debug[['Execution Timestamp', 'Info Type', 'Debug Message']] = df_split.iloc[:, [0, 1, 2]]

    # Trim whitespaces from each column
    df_debug['Execution Timestamp'] = df_debug['Execution Timestamp'].str.strip()
    df_debug['Info Type'] = df_debug['Info Type'].str.strip()
    df_debug['Debug Message'] = df_debug['Debug Message'].str.strip()
    
    df_debug = df_debug.drop(columns=['Log Message'])

    return df_debug

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
