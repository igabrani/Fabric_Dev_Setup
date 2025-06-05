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

# # Superclass

# MARKDOWN ********************

# ## Pandas Superclass

# CELL ********************

class SilverWrangler:

    # Creating a dictionary for the postal code to province translation
    PostalCodeCSV = pd.read_csv(bronze_mount_paths['file_path'] + '/Files/ohds/mapping/Mapping_Postal_Code_PT.csv')
    PostalCodeMapping = PostalCodeCSV.set_index('Postal_Code')['PT'].to_dict()

    def timer(func):
        """
        Function decorator to measure execution time of functions
        """
        def wrapper(*args, **kwargs):
            start_time = time.time()  # Start time
            result = func(*args, **kwargs)  # Execute the function
            end_time = time.time()  # End time
            execution_time = end_time - start_time  # Calculate execution time
            print(f'{func.__name__} executed in: {execution_time} seconds')
            return result
        return wrapper


    @timer
    @staticmethod
    def _add_characters(df, colName, character, positions):

        def modify_string(s):
            if pd.isnull(s):  # Handle NaN or null values
                return s
            # Convert positions to a set to avoid duplicates and sort them
            positions_set = sorted(set(positions))
            # Insert characters at specified positions
            result = []
            for i, c in enumerate(s):
                # Add the character if the index matches a position
                if i in positions_set:
                    result.append(character)
                result.append(c)
            # Add remaining characters for positions beyond the string length
            for pos in positions_set:
                if pos >= len(s):
                    result.append(character)
            return ''.join(result)

        # Apply the vectorized function
        df[colName] = df[colName].apply(modify_string)

        return df


    @timer
    @staticmethod
    def _remove_whitespace(df):
        """
        Remove leading and trailing whitespaces from all str columns in df

        Parameters:
            df (pd.DataFrame)

        Example:
            data = {
                'Provider_Facility_ID': ['000376033 ','  000376033'],
                'Provider_ID': ['000822786  ',' 00084786'],
                'Procedure_Code': [' 31310','31320    '],
            }
            df = pd.DataFrame(data)
            print(df)

            def _remove_whitespace(df):
                cols = df.select_dtypes(include = ['object', 'string']).columns
                df[cols] = df[cols].apply(lambda x: x.str.strip())
                return df

            out = _remove_whitespace(df)
            print(out) #-->  Provider_Facility_ID  Provider_ID Procedure_Code
                            0           000376033   000822786            31310
                            1            000376033     00084786      31320    
                            Provider_Facility_ID Provider_ID Procedure_Code
                            0            000376033   000822786          31310
                            1            000376033    00084786          31320
        """
        cols = df.select_dtypes(include = ['object', 'string']).columns
        df[cols] = df[cols].apply(lambda x: x.str.strip())
        return df

    @timer
    @staticmethod
    def _replace_values(df, colNames, mapping = {'N/A': None}, default_value = "Unknown"):
        """
        Replace values using the dictionary passed or use default mapping to set {'N/A': None},
        apply .fillna() set to 'Unknown',
        replace '' with 'Unknown'
        *Need to include {'N/A': None} in dictionary if required

        Parameters:
            df (pd.DataFrame)
            colNames (str): column(s) can be single or list [] of strings to apply replacement 
            mapping (dict): dictionary with the replacements mapped
            default_value (str): default for unmapped

        Example:
            lst_PT = {
                'BC':'British Columbia',
                'QC':'Quebec',
            }

            data = {
                'Member_ID': ['01','02','03','04','05','06','07'],
                'Member_PT': ['BC','QC',None,float('nan'),None,'','  ']
            }
            df = pd.DataFrame(data)

            def _replace_values(df, colNames, mapping = {'N/A': None}, default_value = "Unknown"):
                df[colNames] = df[colNames].replace(mapping)
                df[colNames] = df[colNames].fillna(default_value)
                df[colNames] = df[colNames].replace(r'^\s*$', default_value, regex = True)
                return df
            out = _replace_values(df, 'Member_PT',lst_PT)
            print(out) #-->  Member_ID         Member_PT
                        0        01  British Columbia
                        1        02            Quebec
                        2        03           Unknown
                        3        04           Unknown
                        4        05           Unknown
                        5        06           Unknown
                        6        07           Unknown
        """
        df[colNames] = df[colNames].replace(mapping)
        df[colNames] = df[colNames].fillna(default_value)
        df[colNames] = df[colNames].replace(r'^\s*$', default_value, regex = True)
        return df

    @timer
    @staticmethod
    def _add_leading_zeros(df, colName, len):
        """
        Add leading zeros to ensure str len is the same for cols with fixed-length 

        Parameters:
            df (pd.DataFrame)
            colName (str): column to add leading zeros
            len (int): length of padding required

        Example:
            data = {
                'Provider_ID': ['01111','022','0333','0554','0555','0336']
            }
            df = pd.DataFrame(data)

            def _add_leading_zeros(df, colName, len):
                df[colName] = df[colName].str.zfill(len)
                return df

            out = _add_leading_zeros(df, 'Provider_ID', 9)
            print(out) #-->  Provider_ID
                        0   000001111
                        1   000000022
                        2   000000333
                        3   000000554
                        4   000000555
                        5   000000336
        """
        df[colName] = df[colName].str.zfill(len)
        return df

    @timer
    @staticmethod
    def _cast_yes_no_to_bool(df, colName):
        """

        Parameters:
            df (pd.DataFrame)
            colName (str):column with two possible values

        Example:
            data = {
                'Member_ID': ['01','02','03','04'],
                'Coordination_of_Benefits_Indicator': ['Y','N','Y','Y']
            }
        df = pd.DataFrame(data)

        def _cast_yes_no_to_bool(df, colName):
            mapping = {'y': True, 'n': False}
            df[colName] = df[colName].map(lambda x: mapping.get(str(x).lower()[0], None))
            return df

        out = _cast_yes_no_to_bool(df,'Coordination_of_Benefits_Indicator')
        print(out) #-->  Member_ID  Coordination_of_Benefits_Indicator
                    0        01                                True
                    1        02                               False
                    2        03                                True
                    3        04                                True
        """
        mapping = {'y': True, 'n': False}
        # use map with a lambda to extract the first letter and apply the mapping
        df[colName] = df[colName].map(lambda x: mapping.get(str(x).lower()[0], None))
        return df

    @timer
    @staticmethod
    def _convert_seconds_to_minutes(df, newColName, colName):
        """
        Convert seconds to minutes
         
        Parameters:
            df (pd.DataFrame)
            newColName (float):
            colName (int):

        Example:
            data = {
                'Queue_Category': ['Member','Provider','Member','Member'],
                'Average_Handle_Time': [426,600,214,30]
            }
            df = pd.DataFrame(data)

            def _convert_seconds_to_minutes(df, newColName, colName):
                df[newColName] = round(df[colName] / 60, 1)
                return df

            out = _convert_seconds_to_minutes(df, newColName = 'Average_Handle_Time_Mins', colName = 'Average_Handle_Time')
            print(out) #-->    Queue_Category  Average_Handle_Time  Average_Handle_Time_Mins
                            0         Member                  426                       7.1
                            1       Provider                  600                      10.0
                            2         Member                  214                       3.6
                            3         Member                   30                       0.5

            out.dtypes #--> 
                    Queue_Category               object
                    Average_Handle_Time           int64
                    Average_Handle_Time_Mins    float64
        """
        df[newColName] = round(df[colName] / 60, 1)
        #df[newColName] = df[colName] / 60
        return df


    @timer
    @staticmethod
    def _clean_numeric_columns(df, colNames):


        for col in colNames:
            # Check if the column exists
            if col not in df.columns:
                print(f"Warning: Column '{col}' does not exist in the DataFrame. Skipping.")
                continue
            
            # Check if the column is of type object (string-like)
            if df[col].dtype == 'object':
                # Remove non-numeric characters, replace empty strings, and convert to float
                df[col] = (
                    df[col]
                    .str.replace(r'[^\d.]+', '', regex=True)  # Remove non-numeric characters
                    .replace('', np.nan)                     # Replace empty strings with NaN
                    .astype(float)                           # Convert to float
                )
            else:
                print(f"Warning: Column '{col}' is not of type object. Skipping.")

        return df


    @timer
    @staticmethod
    def _create_group_column_map(df, newColName, colName, mapping):
        """
        """
        df[newColName] = df[colName].map(mapping)
        return df
   
    @timer
    @staticmethod
    def _create_group_column_num(df, newColName, colName, ranges, labels, default_label = "Unknown"):
        """
        Categorize values into labels based on provided ranges, if value is falls outside of range assign 'Unknown'

        Parameters:
            df (pd.DataFrame)
            newColName (str, object): column created with labels
            colName (int): column with values to categorize
            ranges: define boundaries for labels (e.g. (0, 17) -> 'Under 18')
            labels: assigned to each range

        Example:
            data = {
                'Member_ID': ['01','02','03'],
                'Age': [150,27,70]
            }
            df = pd.DataFrame(data)

            out = _create_group_column_num(df, newColName = 'Age_Group', colName = 'Age', ranges = [(0, 17), (18, 64), (65, 69), (70, 71), (72, 76), (77, 86), (87,)], labels = ['Under 18', '18-64', '65-69', '70-71', '72-76', '77-86', '87+'])
            print(out) #-->   Member_ID  Age Age_Group
                            0        01  150       87+
                            1        02   27     18-64
                            2        03   64     18-64
        """
        # create conditions dynamically based on the provided ranges
        conditions = [(df[colName].between(r[0], r[1]) if len(r) == 2 else df[colName] >= r[0]) for r in ranges]
        # assign the labels based on the conditions
        df[newColName] = np.select(conditions, labels, default = default_label)
        return df

    @timer
    @staticmethod
    def _create_sum_column(df, newColName, colNames):
        """
        Sum values across column list to generate total

        Parameters:
            df (pd.DataFrame)
            newColName (float): column with total
            colNames (float): List of col names to sum ['col1', 'col2']
        """
        df[newColName] = df[colNames].sum(axis = 1)
        return df

    @timer
    @staticmethod
    def _create_province_column_from_postal_code(df, newColName, colName, mapping):
        """
        Create province and territory based on first element or first three elements in postal code

        Parameters:
            df (pd.DataFrame)
            newColName (str): create column for province or territory 
            colName (str): column with postal codes
            mapping (dict): map postal code to province or territory

        Example:
            PostalCodeMapping = {
                'A':'NL',
                'X0A':'NU'
            }
            data = {
                'Member_ID': ['01','02'],
                'Member_Postal_Code': ['A1A 0A1','X0A 0A1']
            }
            df = pd.DataFrame(data)

            out = _create_province_column_from_postal_code(df, 'Member_PT', 'Member_Postal_Code', PostalCodeMapping)
            print(out) #-->Member_ID Member_Postal_Code Member_PT
                        0        01            A1A 0A1        NL
                        1        02            X0A 0A1        NU
        """
        df[newColName] = df[colName].apply(
            lambda x: mapping.get(x[:3], mapping.get(x[0])) if pd.notnull(x) else None
        )
        return df

    @timer
    @staticmethod
    def _create_FSA_column(df, newColName, colName):
        """
        Create FSA from postal code using first three elements

        Parameters:
            df (pd.DataFrame)
            newColName (str): column with FSA
            colName (str): column with postal codes

        Example:
            data = {
                'Member_ID': ['01','02'],
                'Member_Postal_Code': ['A1A 0A1','X0A 0A1']
            }
            df = pd.DataFrame(data)

            out = _create_FSA_column(df, 'Member_FSA', 'Member_Postal_Code')
            print(out) #-->  Member_ID Member_Postal_Code Member_FSA
                        0        01            A1A 0A1        A1A
                        1        02            X0A 0A1        X0A
        """
        df[newColName] = df[colName].str[:3]
        return df

    @timer
    @staticmethod
    def _create_area_type_column(df, newColName, colName):
        """
        Create rural, urban indicator from second element in postal code {'0 = rural','1 = urban'}

        Parameters:
            df (pd.DataFrame)
            newColName (str): column with {'rural','urban'} indicator
            colName (str): column with postal codes

        Example:
            data = {
                'Member_ID': ['01','02'],
                'Member_Postal_Code': ['A1A 0A1','X0A 0A1']
            }
            df = pd.DataFrame(data)

            out = _create_area_type_column(df, 'Member_FSA', 'Member_Postal_Code')
            print(out) #--> Member_ID Member_Postal_Code Member_FSA
                        0        01            A1A 0A1      Urban
                        1        02            X0A 0A1      Rural
        """
        df[newColName] = np.where(df[colName].str[1] == "0", "Rural", "Urban")
        return df

    @timer
    @staticmethod
    def _create_claim_date_column(df):
        """
        Create claim date from claim reference number, if [:6] is NaT set to '2262-04-11'

        Parameters:
            df (pd.DataFrame)
            Claim_Reference_Number (str):
            Claim_Date ():

        Example:
            data = {
                'Member_ID': ['01','02','03'],
                'Claim_Reference_Number': ['190624-BAA24-00','19062B-BAA24-00','000600-BAA24-00']
            }
            df = pd.DataFrame(data)

            def _create_claim_date_column(df):
                # Use vectorized operations
                valid_mask = df['Claim_Reference_Number'].str[:6].str.isdigit()
                        
                # Apply date conversion only for valid entries
                df['Claim_Date'] = pd.to_datetime(df.loc[valid_mask, 'Claim_Reference_Number'].str[:6], format='%d%m%y', errors='coerce')
                df['Claim_Date'] = df['Claim_Date'].fillna(pd.to_datetime('2262-04-11'))
                return df

            out = _create_claim_date_column(df)
            print(out) #--> Member_ID Claim_Reference_Number Claim_Date
                        0        01        190624-BAA24-00 2024-06-19
                        1        02        19062B-BAA24-00 2262-04-11
                        2        03        000600-BAA24-00 2262-04-11
            out.dtypes #-->
                Member_ID                         object
                Claim_Reference_Number            object
                Claim_Date                datetime64[ns]
        """
        valid_mask = df['Claim_Reference_Number'].str[:6].str.isdigit()
        df['Claim_Date'] = pd.to_datetime(df.loc[valid_mask, 'Claim_Reference_Number'].str[:6], format='%d%m%y', errors='coerce')
        # df['Claim_Date'] = df['Claim_Date'].fillna(pd.to_datetime('2262-04-11'))
        return df

    @timer
    @staticmethod
    def _create_enrolled_status_column(df):
        """
        Set column enrolled status to "Y" for data source: PP08

        Parameters:
            df (pd.DataFrame)
        """
        df['Enrolled_Status'] = True
        return df

    
    @timer
    @staticmethod
    def _create_id_column_by_grouping_na_with_previous_value(df, groupColName, newColName = 'ID'):

        df[newColName] = df[groupColName].notna().cumsum()

        return df


    @timer
    @staticmethod
    def _combine_columns_into_new_column(df, newColName, oldColNames):

        df[newColName] = df[oldColNames].bfill(axis=1).iloc[:, 0]

        return df


    @timer
    @staticmethod
    def _explode_column(df, oldColName, regexString):
        
        # Split the 'QC_Code' column by whitespace while keeping '##### AA' together
        df_exploded = df[oldColName].str.extractall(regexString)[0].reset_index()  # Extract matching patterns
        df_exploded.columns = ['OriginalIndex', 'MatchNumber', oldColName]  # Rename columns for clarity

        # Merge back with the original DataFrame to include other columns
        df_merged = df_exploded.merge(df, how = 'right', left_on = 'OriginalIndex', right_index = True, suffixes = ('_new', '_original')).reset_index(drop = True)

        return df_merged


    @timer
    @staticmethod
    def _clean_whitespace_between_regex_sections(df, oldColName, regexString):

        df[oldColName] = df[oldColName].str.replace(regexString, r"\1 \2", regex = True)

        return df

    
    @timer
    @staticmethod
    def _expand_column(df, newColNames, oldColName, numOfSplits, separator):

        df[newColNames] = df[oldColName].str.split(pat = separator, n = numOfSplits, expand = True)

        return df


    @timer
    @staticmethod
    def _add_to_additional_notes_column(df):

        #mask_ca = df['Code'].notna()
        mask_qc = df['QC_Code_orig'].notna()

        #df.loc[mask_ca, 'Additional_Notes_ca'] = df.loc[mask_ca, 'Code'].str.split(n = 1).str[1]
        df.loc[mask_qc, 'Additional_Notes_qc'] = df['QC_Code_orig'].str.extract(r"(?:\d{5}\s+[A-Z]{2}\s*)+(.*)")[0].str.strip()

        #df['Additional_Notes_ca'].replace('', np.nan, inplace = True)
        df['Additional_Notes_qc'].replace('', np.nan, inplace = True)

        df['Additional_Notes'] = df['Additional_Notes_ca'].fillna('') \
                                                          .str.cat(df['Additional_Notes_qc'].fillna(''), sep='|') \
                                                          .str.strip('|') 

        df['Additional_Notes'].replace('', np.nan, inplace = True)  

        return df


    @timer
    @staticmethod
    def _create_column_name_column(df, newColName, oldColNames, separator = '|'):

        df[newColName] = df[oldColNames].apply(
            lambda row: separator.join([col for col in oldColNames if pd.notna(row[col])]),
            axis = 1
        )

        return df


    @timer
    @staticmethod
    def _explode_codes(df):

        # Rows where B is NULL
        df_qc_null = df[df['QC_Code_new'].isna()].copy()
        df_qc_null['Code_Combined'] = df_qc_null['CA_Code_Base']
        df_qc_null['QC_Flag'] = "CAN"

        # Rows where A is NULL
        df_ca_null = df[df['CA_Code_Base'].isna()].copy()
        df_ca_null['Code_Combined'] = df_ca_null['QC_Code_new']
        df_ca_null['QC_Flag'] = "QC"

        # Rows where neither A nor B is NULL - duplicates the rows
        df_both_non_null = df[df['CA_Code_Base'].notna() & df['QC_Code_new'].notna()].copy()

        df_both_non_null_ca = df_both_non_null.copy()
        df_both_non_null_ca = df_both_non_null_ca.drop_duplicates(subset = 'ID')
        df_both_non_null_ca['Code_Combined'] = df_both_non_null_ca['CA_Code_Base']
        df_both_non_null_ca['QC_Flag'] = "CAN"

        df_both_non_null_qc = df_both_non_null.copy()
        df_both_non_null_qc['Code_Combined'] = df_both_non_null_qc['QC_Code_new']
        df_both_non_null_qc['QC_Flag'] = "QC"

        # Concatenate all the DataFrames
        result_df = pd.concat([df_qc_null, df_ca_null, df_both_non_null_ca, df_both_non_null_qc], ignore_index = True)

        return result_df


    @timer
    @staticmethod
    def _fill_na_column(df, oldColNames, fillMethod = 'ffill'):

        df[oldColNames] = df[oldColNames].fillna(method = fillMethod)

        return df


    @timer
    @staticmethod
    def _fix_qc_association(df):

        df.loc[df['QC_Flag'] == "QC", 'Association'] = "ACDQ"
        df.loc[(df['QC_Flag'] == "QC") & (df['Specialty'] != "GP"), 'Association'] = "FDSQ" 

        return df


    @timer
    @staticmethod
    def _unify_columns(df, newColName, oldCol1, oldCol2):

        df[newColName] = df[oldCol1].fillna(df[oldCol2])

        return df

    @timer
    @staticmethod
    def _combine_columns(df, newColName, oldColNames, separator = '|'):

            # Replace empty strings with NaN to treat them as nulls
            df[oldColNames] = df[oldColNames].replace('', np.nan)
            
            # Use str.cat on multiple columns, filtering out NaNs row-wise
            df[newColName] = df[oldColNames].apply(lambda row: separator.join(row.dropna()), axis = 1)
            
            # Replace empty strings with NaN if all columns were originally empty/null
            #df[newColName] = df[newColName].replace('', np.nan, inplace = True)
            
            return df




    @timer
    @staticmethod
    def _replace_value_in_column_names(df, newValue, oldValue):

        df.columns = df.columns.str.replace(oldValue, newValue, regex = False)

        return df

    
    @timer
    @staticmethod
    def _fix_specialty(df):
        
        df.loc[df['Specialty'] == "PA", 'Specialty'] = "PD"
        df.loc[df['Additional_Notes'] == "only for OS", 'Specialty'] = "OS"
        df.loc[df['Association'] == "DAC", 'Specialty'] = "DT"
        df.loc[df['Association'] == "CDHA", 'Specialty'] = "HY"

        return df



    @timer
    @staticmethod
    def _replace_value_in_column_A_if_column_B_not_null(df, columnB, columnA):

        df.loc[df[columnB].notna(), columnA] = df[columnB]

        return df


    @timer
    @staticmethod
    def _drop_duplicates(df, subsetValue, keepValue = "first"):

        df = df.drop_duplicates(subset = subsetValue, keep = keepValue)

        return df


    @timer
    @staticmethod
    def _create_lowest_age_column(df):
        
        # Define conditions and choices
        conditions = [
            df['Age_Group'] == "Under 18",
            df['Age_Group'] == "18-64",
            df['Age_Group'] == "65-69",
            df['Age_Group'] == "70-71",
            df['Age_Group'] == "72-76",
            df['Age_Group'] == "77-86",
            df['Age_Group'] == "87+"
        ]

        choices = [0, 18, 65, 70, 72, 77, 87]

        df['Lowest_Age'] = np.select(conditions, choices, default = 0)

        return df

        

    @timer
    @staticmethod
    def _create_date_column(df, newColName, dateString):

        df[newColName] = pd.to_datetime(dateString)

        return df

    @timer
    @staticmethod
    def _cast_column_dtype(df, colName, newDType):

        df[colName] = df[colName].astype(newDType)

        return df


    @timer
    @staticmethod
    def _cast_null_strings_to_int(df, colName):

        df[colName] = pd.to_numeric(df[colName], errors='coerce')  # Coerce invalid values to NaN
        df[colName] = df[colName].fillna(0).astype("Int64")  

        return df

    
    @timer
    @staticmethod
    def _sort_by_columns(df, sortingColumns):

        df = df.sort_values(by = sortingColumns)

        return df

    
    @timer
    @staticmethod
    def _create_qc_flag_cl90(df):

        df['QC_Flag'] = "CAN"
        df.loc[df['Provider_Province'] == "QC", 'QC_Flag'] = "QC"

        return df


    @timer
    @staticmethod
    def _create_os_flag_cl90(df, newColName, oldColName):

        df[newColName] = df[oldColName].notna() & (df['Specialty'] == "OS")

        return df

    
    @timer
    @staticmethod
    def _create_code_join_cl90(df, newColName, procedureCodeCol, OSFlagCol):

        # Initialize new column with None for rows where procedureCodeCol is null
        df[newColName] = np.where(
            pd.isnull(df[procedureCodeCol]), 
            None,  # Assign None where procedureCodeCol is null
            np.where(
                df['Specialty'].isin(['DT', 'HY']),
                df[procedureCodeCol] + '-' + df['Specialty'],
                np.where(
                    (df['QC_Flag'] == 'QC') | (df[OSFlagCol]),
                    df[procedureCodeCol] + '-' + df['QC_Flag'] + '-' + df['Specialty'],
                    df[procedureCodeCol] + '-' + df['QC_Flag']
                )
            )
        )

        return df


    @timer
    @staticmethod
    def _create_code_join_column_procedure_codes(df):

        # Initialize new column with None for rows where procedureCodeCol is null
        df['Code_Join'] = np.where(
            df['Specialty'].isin(['DT', 'HY']),
            df['Code_Combined_Base'] + '-' + df['Specialty'],
            np.where(
                (df['QC_Flag'] == 'QC') | (df['Specialty'] == 'OS'),
                df['Code_Combined_Base'] + '-' + df['QC_Flag'] + '-' + df['Specialty'],
                df['Code_Combined_Base'] + '-' + df['QC_Flag']
            ) 
        )
        
        return df

    
    @timer
    @staticmethod
    def _create_cumsum_column_by_group(df, newColName, sumCol, groupCol):
        
        df[newColName] = df.groupby(groupCol)[sumCol].cumsum()
        
        return df

    
    @timer
    @staticmethod
    def _subtract_num_from_total_by_group(df, newColName, totalCol, numCol, groupCol):
        
        sum_total_by_group = df.groupby(groupCol)[totalCol].transform('sum')

        df[newColName] = sum_total_by_group - df[numCol]
        
        return df


    @timer
    @staticmethod
    def _create_fr_column(df, newColName, oldColName, mapping):
        
        df[newColName] = df[oldColName].replace(mapping)
        
        return df


    @timer
    @staticmethod
    def _sort_and_rename_cols(df, keepCols, renameCols):
        """
        Keep columns listed in Wrangler class code for each data source (e.g. CL90Wrangler.KeepCols)

        Parameters:
            df (pd.DataFrame)
            keepCols: list of columns in Wrangler class code
            renameCols: dictionary in Wrangler class code
        """
        df = df[keepCols].rename(columns = renameCols)
        return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PySpark Superclass

# CELL ********************

class SilverWranglerPySpark:


    def timer(func):
        """
        Function decorator to measure execution time of functions
        """
        def wrapper(*args, **kwargs):
            start_time = time.time()  # Start time
            result = func(*args, **kwargs)  # Execute the function
            end_time = time.time()  # End time
            execution_time = end_time - start_time  # Calculate execution time
            print(f'{func.__name__} executed in: {execution_time} seconds')
            return result
        return wrapper


    @timer
    @staticmethod
    def _replace_values(df, colNames, mapping={'N/A': None}, default_value="Unknown"):
        """
        Replace values using the dictionary passed or use default mapping to set {'N/A': None},
        apply fillna set to 'Unknown',
        replace '' or whitespace-only strings with 'Unknown'.

        Parameters:
            df (DataFrame): PySpark DataFrame.
            colNames (str or list): Column(s) to apply replacement. Can be a single column or a list of columns.
            mapping (dict): Dictionary with the replacements mapped.
            default_value (str): Default for unmapped values.

        Returns:
            DataFrame: Transformed PySpark DataFrame.
        """
        # Ensure colNames is a list for uniform processing
        if isinstance(colNames, str):
            colNames = [colNames]
        
        for col in colNames:
            # Replace values based on the mapping
            mapping_expr = F.create_map([F.lit(k) for pair in mapping.items() for k in pair])
            df = df.withColumn(
                col,
                F.when(F.col(col).isin(list(mapping.keys())), mapping_expr.getItem(F.col(col)))
                .otherwise(F.col(col))
            )
            
            # Replace null or NaN with default_value
            df = df.withColumn(
                col,
                F.when(F.col(col).isNull(), default_value)
                .otherwise(F.col(col))
            )
            
            # Replace empty or whitespace-only strings with default_value
            df = df.withColumn(
                col,
                F.when(F.trim(F.col(col)) == "", default_value)
                .otherwise(F.col(col))
            )
        
        return df


    @timer
    @staticmethod
    def _create_string_col(df, newColName, string):

        df = df.withColumn(newColName, F.lit(string))

        return df


    @timer
    @staticmethod
    def _create_date_col(df, newColName, date):

        df = df.withColumn(newColName, F.lit(date).cast('date'))

        return df


    @timer
    @staticmethod
    def _create_time_now_col(df, newColName):

        df = df.withColumn(newColName, F.from_utc_timestamp(F.current_timestamp(), "America/New_York"))

        return df

    
    @timer
    @staticmethod
    def _adjust_timestamp_from_UTC(df, colName):

        # Change if needing to compare to ESDC numbers
        df = df.withColumn(colName, F.from_utc_timestamp(col(colName), "America/New_York"))
        #df = df.withColumn(colName, F.col(colName) - F.expr("INTERVAL 5 HOURS"))

        return df


    
    @timer
    @staticmethod
    def _drop_duplicates_while_keeping_null_values(df, duplicateCol):

        null_rows = df.filter(col(duplicateCol).isNull())

        non_null_rows = df.filter(col(duplicateCol).isNotNull())

        new_df = non_null_rows.dropDuplicates([duplicateCol])

        result_df = new_df.union(null_rows)

        return result_df


    @timer
    @staticmethod
    def _combine_duplicates_while_keeping_null_values(df, duplicateCol):

        null_rows = df.filter(col(duplicateCol).isNull())

        non_null_rows = df.filter(col(duplicateCol).isNotNull())

        # Group by Column A
        grouped_df = non_null_rows \
            .groupBy(duplicateCol).agg(
                *[
                    # Concatenate unique values of each column into a string
                    F.concat_ws("|", F.collect_set(col)).alias(col)
                    for col in non_null_rows.columns if col != duplicateCol
                ]
            )

        result_df = grouped_df.union(null_rows)

        return result_df


    @timer
    @staticmethod
    def _create_year_diff_column(df, newColName, earlyDate, laterDate):

        df = df.withColumn(
            newColName,
            F.floor(F.datediff(col(laterDate), col(earlyDate)) / 365.25)
        )

        return df

    
    @timer
    @staticmethod
    def _create_ghost_parent_column(df):

        df = df.withColumn(
            'ghost_parent_flag',
            F.when(
                col('esdc_coveragestart') == col('esdc_coverageend')
                , 1)
            .otherwise(0)
        )

        return df

    
    @timer
    @staticmethod
    def _create_eligible_column(df):

        df = df.withColumn(
            'eligible_flag',
            F.when(
                (col('ghost_parent_flag') != 1)
                & (col('esdc_applicanttype_en').isin(["Applicant", "Dependent"]))
                , 1)
            .otherwise(0)
        )

        return df


    @timer
    @staticmethod
    def _create_enrolled_column(df):

        df = df.withColumn(
            'enrolled_flag',
            F.when(
                (col('eligible_flag') == 1)
                & (col('esdc_enroledon').isNotNull())
                , 1)
            .otherwise(0)
        )

        return df
    
    @timer
    @staticmethod
    def _create_real_address_columns(df):

        condition = (col('esdc_homeaddressprovinceterritorystate_en').isNotNull()) & (col('esdc_homeaddressprovinceterritorystate_en') != "Unknown")

        # Create multiple columns using the same condition
        df = df.withColumn('real_address_street', when(condition, col('esdc_homeaddressstreet')).otherwise(col('esdc_mailingaddressstreet'))) \
            .withColumn('real_address_unit', when(condition, col('esdc_homeaddressapartmentunitnumber')).otherwise(col('esdc_mailingaddressapartmentunitnumber'))) \
            .withColumn('real_address_city', when(condition, col('esdc_homeaddresscity')).otherwise(col('esdc_mailingaddresscity'))) \
            .withColumn('real_address_postal_code', when(condition, col('esdc_homeaddresspostalzipcode')).otherwise(col('esdc_mailingaddresspostalzipcode'))) \
            .withColumn('real_address_pt', when(condition, col('esdc_homeaddressprovinceterritorystate_en')).otherwise(col('esdc_mailingaddressprovinceterritorystate_en'))) \
            .withColumn('real_address_country', when(condition, col('esdc_homeaddresscountry_en')).otherwise(col('esdc_mailingaddresscountry_en'))) \
            .withColumn('home_or_mailing_address', when(condition, lit("Home")).otherwise(lit("Mailing")))

        return df

    @timer
    @staticmethod
    def _create_insurance_details_join_id(df):

        df = df.withColumn('insurance_details_join_id', F.concat(df['dfp_incremental_id'], lit("|"), df['source']))

        return df


    @timer
    @staticmethod
    def _cast_column(df, colName, dtype):
        
        df = df.withColumn(
            colName,
            df[colName].cast(dtype)
        )

        return df


    @timer
    @staticmethod
    def _cast_string_column_to_date(df, colName):

        df = df.withColumn(
            colName,
            F.to_date(colName, "yyyy-MM-dd")
        )

        return df


    @timer
    @staticmethod
    def _cast_string_column_to_timestamp_then_date(df, colName):

        df = df.withColumn(
            colName,
            F.to_date(F.to_timestamp(colName, "yyyy-MM-dd HH:mm:ss"))
        )

        return df

    
    @timer
    @staticmethod
    def _cast_string_column_to_timestamp(df, colName):

        df = df.withColumn(
            colName,
            F.to_timestamp(colName, "yyyy-MM-dd HH:mm:ss")
        )

        return df


    @timer
    @staticmethod
    def _cast_yes_no_to_bool(df, colName):

        mapping = {'y': True, 'n': False}
        # use map with a lambda to extract the first letter and apply the mapping
        df[colName] = df[colName].map(lambda x: mapping.get(str(x).lower()[0], None))
        return df


    @timer
    @staticmethod
    def _get_cols_to_front(df, colsToFront):
        original = df.columns
        # Filter to present columns
        colsToFront = [c for c in colsToFront if c in original]
        # Keep the rest of the columns and sort it for consistency
        columns_other = list(set(original) - set(colsToFront))
        columns_other.sort()
        # Apply the order
        df = df.select(*colsToFront, *columns_other)

        return df


    @timer
    @staticmethod
    def _sort_and_rename_cols(df, keepCols, renameCols):
        """
        Keep specified columns and rename them according to a dictionary.

        Parameters:
            df (DataFrame): Input PySpark DataFrame
            keepCols (list): List of columns to keep
            renameCols (dict): Dictionary mapping old column names to new column names
        
        Returns:
            DataFrame: Transformed PySpark DataFrame
        """
        # Select only the columns in keepCols
        df = df.select([col(c) for c in keepCols])
        
        # Rename columns using the renameCols dictionary
        df = df.select(
            [col(c).alias(renameCols[c]) if c in renameCols else col(c) for c in keepCols]
        )
        
        return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Subclasses

# MARKDOWN ********************

# ## Initialize Registry

# CELL ********************

class SourceRegistry:
    
    def __init__(self):
        self.source_type = {}
        self.read_schema = {}
        self.date_cols = {}
        self.read_root = {}
        self.write_schema = {}
        self.write_path = {}
        self.write_behaviour = {}
        self.apply_transformations = {}
        self.apply_joins = {}


    def register_source(self, source, write_behaviour):

        source_key = source.replace("/", "_")

        if source == "esdc/members_eligible":
            self.source_type[source] = "parquet"
        else:
            self.source_type[source] = "csv"
        
        self.read_schema[source] = globals()[f"get_read_schema_{source_key}"]
        self.date_cols[source] = globals()[f"get_date_cols_{source_key}"]
        self.read_root[source] = f"{bronze_mount_paths['file_path']}/Files/{source}/"

        self.write_schema[source] = globals()[f"get_write_schema_{source_key}"]
        self.write_path[source] = self._make_get_write_path_func(source)
        self.write_behaviour[source] = write_behaviour

        wrangler = globals()[f"wrangler_{source_key}"]
        self.apply_transformations[source] = getattr(wrangler, 'apply_transformations')
        
        # Only add apply_joins if it exists
        if hasattr(wrangler, "apply_joins"):
            self.apply_joins[source] = getattr(wrangler, "apply_joins")


    def _make_get_write_path_func(self, source):
        def get_write_path():
            return f"{silver_mount_paths['file_path']}/Tables/{source}"
        return get_write_path


    def get(self, source):

        return {
            "source_type": self.source_type[source],
            "read_schema": self.read_schema[source],
            "date_cols": self.date_cols[source],
            "read_root": self.read_root[source],
            "write_schema": self.write_schema[source],
            "write_path": self.write_path[source],
            "write_behaviour": self.write_behaviour[source],
            "apply_transformations": self.apply_transformations[source],
            "apply_joins": self.apply_joins.get(source)
        }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

registry = SourceRegistry()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## CRA

# MARKDOWN ********************

# ### copay_tiers

# CELL ********************

def get_read_schema_cra_copay_tiers():
    return {
    'SIN': str,
    'CO_PAY_TIER': str
    }

def get_date_cols_cra_copay_tiers():
    return []

def get_write_schema_cra_copay_tiers():
    return StructType ([
        StructField('SIN', StringType(), True),
        StructField('Member_ID', StringType(), True),
        StructField('CRA_Co_Pay_Tier', StringType(), True)
    ])
    

class wrangler_cra_copay_tiers(SilverWrangler):
    '''
    Class to enrich CRA Co-Pay Tier data into a standardized form.
    '''

    keepCols = [
        'SIN',
        'esdc_clientnumber',
        'CO_PAY_TIER'
    ]

    renameCols = {
        'esdc_clientnumber': 'Member_ID',
        'CO_PAY_TIER': 'CRA_Co_Pay_Tier'
    }

    @classmethod
    def apply_transformations(cls, df):

        return df


    @classmethod
    def apply_joins(cls, df):

        # Read in the dataframes to be joined
        df_members = spark.sql(f'SELECT esdc_socialinsurancenumber, esdc_clientnumber FROM {silver_lakehouse_name}.esdc.dim_members').toPandas()
        df_members = df_members.drop_duplicates(subset = ['esdc_socialinsurancenumber'])

        # Join members on the column "SIN"
        df_merged = df.merge(df_members, how = 'left', left_on = 'SIN', right_on = 'esdc_socialinsurancenumber')
        df_merged = df_merged.drop_duplicates(subset = ['esdc_clientnumber'])


        # Sort and rename the columns in co-pay
        df_merged = df_merged \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_cra_copay_tiers.keepCols, renameCols = wrangler_cra_copay_tiers.renameCols)

        return df_merged


registry.register_source("cra/copay_tiers", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ESDC

# MARKDOWN ********************

# ### contact_centre

# CELL ********************

def get_read_schema_esdc_contact_centre_stats():
    return {
        'Date': str, # date
        'Language': str,
        'Calls_Answered': float,
        'Calls_Answered_Within_Target': float,
        'Average_Wait_Time': float,
        'Total_Average_Wait_Time': float
    }
    

def get_date_cols_esdc_contact_centre_stats():
    return [
        'Date'
    ]

def get_write_schema_esdc_contact_centre_stats():
    return StructType ([
        StructField('Date', DateType(), True),
        StructField('Language', StringType(), True),
        StructField("Language_FR", StringType(), True),
        StructField('Calls_Answered', DoubleType(), True),
        StructField('Calls_Answered_Within_Target', DoubleType(), True),
        StructField('Average_Wait_Time', DoubleType(), True),
        StructField('Total_Average_Wait_Time', DoubleType(), True)
    ])

    
class wrangler_esdc_contact_centre_stats(SilverWrangler):
    '''
    Class to enrich ESDC Contact Centre data into a standardized form.
    '''

    keepCols = [
        'Date',
        'Language',
        'Language_FR',
        'Calls_Answered',
        'Calls_Answered_Within_Target',
        'Average_Wait_Time',
        'Total_Average_Wait_Time'
    ]

    renameCols = {
    }

    @classmethod
    def apply_transformations(cls, df):

        return df \
            .transform(cls._create_fr_column, newColName = 'Language_FR', oldColName = 'Language', mapping = {'English': 'Anglais', 'French': 'Français'}) \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_esdc_contact_centre_stats.keepCols, renameCols = wrangler_esdc_contact_centre_stats.renameCols)

            
registry.register_source("esdc/contact_centre_stats", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fsa_applications

# CELL ********************

def get_read_schema_esdc_fsa_applications():
    return {
        'Month of Submission': str, #date
        'FSA': str,
        'Province / Territory': str,
        'Age Range': str,
        'Renewal': str,
        'Total Eligible Applicants': float
    }
    

def get_date_cols_esdc_fsa_applications():
    return [
        'Month of Submission'
    ]

def get_write_schema_esdc_fsa_applications():
    return StructType ([
        StructField("Submission_Month", DateType(), True),
        StructField("FSA", StringType(), True),
        StructField("Member_PT", StringType(), True),
        StructField("Age_Range", StringType(), True),
        StructField("Renewal", StringType(), True),
        StructField("Total", LongType(), True)
    ])

    
class wrangler_esdc_fsa_applications(SilverWrangler):
    '''
    Class to enrich ESDC Applications FSA into a standardized form.
    '''

    keepCols = [
        'Month_of_Submission',
        'FSA',
        'Province_/_Territory',
        'Age_Range',
        'Renewal',
        'Total_Eligible_Applicants'
    ]

    renameCols = {
        'Month_of_Submission': 'Submission_Month',
        'Province_/_Territory': 'Member_PT',
        'Total_Eligible_Applicants': 'Total'
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        return df \
            .transform(cls._replace_values, colNames = ['Province_/_Territory'], mapping = {"Alberta": "AB",
                "British Columbia": "BC",
                "Manitoba": "MB",
                "New Brunswick": "NB",
                "Newfoundland and Labrador": "NL",
                "Nova Scotia": "NS",
                "Northwest Territories": "NT",
                "Nunavut": "NT",
                "Ontario": "ON",
                "Prince Edward Island": "PE",
                "Quebec": "QC",
                "Saskatchewan": "SK",
                "Yukon": "YT",
                "Unknown": "ZZ",
                "Outside Canada": "OC"}) \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_esdc_fsa_applications.keepCols, renameCols = wrangler_esdc_fsa_applications.renameCols)

            
registry.register_source("esdc/fsa_applications", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### pt_applications

# CELL ********************

def get_read_schema_esdc_pt_applications():
    return {
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
    

def get_date_cols_esdc_pt_applications():
    return [
        'Month of Submission'
    ]

def get_write_schema_esdc_pt_applications():
    return StructType([
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

    
class wrangler_esdc_pt_applications(SilverWrangler):
    '''
    Class to enrich ESDC Applications PT into a standardized form.
    '''

    keepCols = [
        'Month_of_Submission',
        'Applicant_Type', 
        'Province_/_Territory',
        'Urban_/_Rural',
        'Member_Area_Type_FR',
        'Age_Range',
        'Language',
        'Language_FR',
        'Application_Received',
        'Application_Completed',
        'Eligibility',
        'Disability',
        'Renewal',
        'Total_Count'
    ]

    renameCols = {
        'Month_of_Submission': 'Submission_Month',
        'Province_/_Territory': 'Member_PT',
        'Urban_/_Rural': 'Member_Area_Type'
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        return df \
            .transform(cls._replace_values, colNames = ['Language'], mapping = {None: "Not specified", "Unknown": "Not specified"}) \
            .transform(cls._replace_values, colNames = ['Province_/_Territory'], mapping = {"Alberta": "AB",
                "British Columbia": "BC",
                "Manitoba": "MB",
                "New Brunswick": "NB",
                "Newfoundland and Labrador": "NL",
                "Nova Scotia": "NS",
                "Northwest Territories": "NT",
                "Nunavut": "NT",
                "Ontario": "ON",
                "Prince Edward Island": "PE",
                "Quebec": "QC",
                "Saskatchewan": "SK",
                "Yukon": "YT",
                "Unknown": "ZZ",
                "Outside Canada": "OC"}) \
            .transform(cls._create_fr_column, newColName = 'Language_FR', oldColName = 'Language', mapping = {'English': 'Anglais', 'French': 'Français', 'Not specified': 'Non precisées'}) \
            .transform(cls._create_fr_column, newColName = 'Member_Area_Type_FR', oldColName = 'Urban_/_Rural', mapping = {'Urban': 'urbaine', 'Rural': 'rurale', 'Unknown': 'inconnue'}) \
            .transform(cls._combine_columns_into_new_column, newColName = 'Disability', oldColNames = ['CRA-Validated_Disability', 'Applicant-Stated_Disability']) \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_esdc_pt_applications.keepCols, renameCols = wrangler_esdc_pt_applications.renameCols)

            
registry.register_source("esdc/pt_applications", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fsa_enrolled

# CELL ********************

def get_read_schema_esdc_fsa_enrolled():
    return {
        'Month of Enrolment': str, #date
        'FSA': str,
        'Province / Territory': str,
        'Age Range': str,
        'Total Enroled Clients': float
    }
    

def get_date_cols_esdc_fsa_enrolled():
    return [
        'Month of Enrolment'
    ]

def get_write_schema_esdc_fsa_enrolled():
    return StructType([
        StructField("Enrollment_Month", DateType(), True),
        StructField("FSA", StringType(), True),
        StructField("Member_PT", StringType(), True),
        StructField("Age_Range", StringType(), True),
        StructField("Total", LongType(), True)
    ])

    
class wrangler_esdc_fsa_enrolled(SilverWrangler):
    '''
    Class to enrich ESDC Enrolled FSA into a standardized form.
    '''

    keepCols = [
        'Month_of_Enrolment',
        'FSA',
        'Province_/_Territory',
        'Age_Range',
        'Total_Enroled_Clients'
    ]

    renameCols = {
        'Month_of_Enrolment': 'Enrollment_Month',
        'Province_/_Territory': 'Member_PT',
        'Total_Enroled_Clients': 'Total'
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        return df \
            .transform(cls._replace_values, colNames = ['Province_/_Territory'], mapping = {"Alberta": "AB",
                "British Columbia": "BC",
                "Manitoba": "MB",
                "New Brunswick": "NB",
                "Newfoundland and Labrador": "NL",
                "Nova Scotia": "NS",
                "Northwest Territories": "NT",
                "Nunavut": "NT",
                "Ontario": "ON",
                "Prince Edward Island": "PE",
                "Quebec": "QC",
                "Saskatchewan": "SK",
                "Yukon": "YT",
                "Unknown": "ZZ",
                "Outside Canada": "OC"}) \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_esdc_fsa_enrolled.keepCols, renameCols = wrangler_esdc_fsa_enrolled.renameCols)

            
registry.register_source("esdc/fsa_enrolled", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### pt_enrolled

# CELL ********************

def get_read_schema_esdc_pt_enrolled():
    return {
        'Month of Enrolment': str, #date
        'Province / Territory': str,
        'Urban / Rural': str,
        'Age Range': str,
        'Language': str,
        'Total Count': float
    }
    

def get_date_cols_esdc_pt_enrolled():
    return [
        'Month of Enrolment'
    ]

def get_write_schema_esdc_pt_enrolled():
    return StructType([
        StructField("Enrolment_Month", DateType(), True),
        StructField("Member_PT", StringType(), True),
        StructField("Member_Area_Type", StringType(), True),
        StructField("Member_Area_Type_FR", StringType(), True),
        StructField("Age_Range", StringType(), True),
        StructField("Language", StringType(), True),
        StructField("Language_FR", StringType(), True),
        StructField("Total_Count", LongType(), True)
    ])

    
class wrangler_esdc_pt_enrolled(SilverWrangler):
    '''
    Class to enrich ESDC Enrolled PT into a standardized form.
    '''

    keepCols = [
        'Month_of_Enrolment',
        'Province_/_Territory',
        'Urban_/_Rural',
        'Member_Area_Type_FR',
        'Age_Range',
        'Language',
        'Language_FR',
        'Total_Count'
    ]

    renameCols = {
        'Month_of_Enrolment': 'Enrolment_Month',
        'Province_/_Territory': 'Member_PT',
        'Urban_/_Rural': 'Member_Area_Type',
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        return df \
            .transform(cls._replace_values, colNames = ['Language'], mapping = {None: "Not specified", "Unknown": "Not specified"}) \
            .transform(cls._replace_values, colNames = ['Province_/_Territory'], mapping = {"Alberta": "AB",
                "British Columbia": "BC",
                "Manitoba": "MB",
                "New Brunswick": "NB",
                "Newfoundland and Labrador": "NL",
                "Nova Scotia": "NS",
                "Northwest Territories": "NT",
                "Nunavut": "NT",
                "Ontario": "ON",
                "Prince Edward Island": "PE",
                "Quebec": "QC",
                "Saskatchewan": "SK",
                "Yukon": "YT",
                "Unknown": "ZZ",
                "Outside Canada": "OC"}) \
            .transform(cls._create_fr_column, newColName = 'Language_FR', oldColName = 'Language', mapping = {'English': 'Anglais', 'French': 'Français', 'Not specified': 'Non precisées'}) \
            .transform(cls._create_fr_column, newColName = 'Member_Area_Type_FR', oldColName = 'Urban_/_Rural', mapping = {'Urban': 'urbaine', 'Rural': 'rurale', 'Unknown': 'inconnue'}) \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_esdc_pt_enrolled.keepCols, renameCols = wrangler_esdc_pt_enrolled.renameCols)

            
registry.register_source("esdc/pt_enrolled", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### members_eligible

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

# CELL ********************

class wrangler_esdc_members_eligible(SilverWranglerPySpark):
    '''
    Class to enrich ESDC Members into a standardized form.
    '''
    
    ptCSV = pd.read_csv(bronze_mount_paths['file_path'] + '/Files/ohds/grouping/Grouping_PT.csv')
    ptMapping = ptCSV.set_index('PT_Long')['Abbreviation'].to_dict()

    keepCols = [
        # Delta info
        'dfp_incremental_id',
        'dfp_change',
        'effective_date',
        'end_date',
        'created_time',
        'source',

        # IDs
        'uniqueid',
        'esdc_familyid',
        'esdc_clientnumber',
        'esdc_firstname',
        'esdc_lastname',
        'esdc_socialinsurancenumber',

        # Flags
        'ghost_parent_flag',
        'eligible_flag',
        'enrolled_flag',

        # Dates
        'esdc_dateofapplication',
        'esdc_attestationcompletedon', # datetime
        'esdc_eligibleonfirst', # date
        'esdc_enroledon', # datetime
        'esdc_batchdate', # datetime
        'esdc_coveragestart', # date
        'esdc_coverageend', # date
        'esdc_dateofbirth', # date
        
        # Personal info
        'esdc_sex_en', # code
        'esdc_applicanttype_en',
        'esdc_basetaxyear',
        'esdc_overagedependent', # 1/0 code, delete en/fr versions
        'esdc_canadianresident', # 1/0 code, delete en/fr versions
        'esdc_sharedcustodyname', # 1/0 code, delete en/fr versions
        'esdc_disabilitytaxcredit', # 1/0 code, delete en/fr versions
        'esdc_copaytier_en', # code
        'esdc_billinggroup',
        
        # Contact info
        'emailaddress',
        'esdc_phonenumber',
        'esdc_preferredlanguage_en', # code
        'esdc_preferredmethodofcommunication_en', # code
        'esdc_maritalstatus_en', # code
        
        # Addresses
        'real_address_street',
        'real_address_unit',
        'real_address_city',
        'real_address_postal_code',
        'real_address_pt',
        'real_address_country',
        'home_or_mailing_address',
        'esdc_homeaddressstreet',
        'esdc_homeaddressapartmentunitnumber',
        'esdc_homeaddresscity',
        'esdc_homeaddresspostalzipcode',
        'esdc_homeaddressprovinceterritorystate_en', # code
        'esdc_homeaddresscountry_en', # code
        'esdc_mailingaddressstreet',
        'esdc_mailingaddressapartmentunitnumber',
        'esdc_mailingaddresscity',
        'esdc_mailingaddresspostalzipcode',
        'esdc_mailingaddressprovinceterritorystate_en', # code
        'esdc_mailingaddresscountry_en', # code

        # Attestation
        'esdc_hasdentalinsurancecoverageprovincialfederal', # 1/0 code, delete en/fr versions
        'esdc_hasdentalinsurancecoverageemployerprivate', # 1/0 code, delete en/fr versions
        'provincial_plan_esdc_name_en', # code
        'provincial_plan_esdc_name_fr', # code
        'federalprogram_escd_name_en', # code
        'federalprogram_escd_name_fr', # code
        
        # Spouse info
        'spouse_esdc_clientnumber',
        'spouse_esdc_firstname',
        'spouse_esdc_lastname',
        'spouse_esdc_socialinsurancenumber',
        'spouse_esdc_dateofbirth', # date
        'spouse_esdc_canadianresident', # 1/0 code, delete en/fr versions

        'insurance_details_join_id'  
    ]


# Removed columns:
    # 1/0 Code:
        #'esdc_overagedependent_en',
        #'esdc_overagedependent_fr',
        #'esdc_hasdentalinsurancecoverageprovincialfederal_en',
        #'esdc_hasdentalinsurancecoverageprovincialfederal_fr',
        #'esdc_hasdentalinsurancecoverageemployerprivate_en',
        #'esdc_hasdentalinsurancecoverageemployerprivate_fr',
        #'esdc_canadianresident_en',
        #'esdc_canadianresident_fr',
        #'esdc_sharedcustodyname_en',
        #'esdc_sharedcustodyname_fr',
        #'esdc_disabilitytaxcredit_en',
        #'esdc_disabilitytaxcredit_fr',
        #'spouse_esdc_canadianresident_en',
        #'spouse_esdc_canadianresident_fr',
    # Language:
        #'esdc_applicanttype',
        #'esdc_applicanttype_fr',
        #'esdc_sex', # code
        #'esdc_sex_fr', # code
        #'esdc_preferredlanguage', # code
        #'esdc_preferredlanguage_fr', # code
        #'esdc_preferredmethodofcommunication', # code
        #'esdc_preferredmethodofcommunication_fr', # code
        #'esdc_maritalstatus', # code
        #'esdc_maritalstatus_fr', # code
        #'esdc_homeaddressprovinceterritorystateid', # code
        #'esdc_homeaddressprovinceterritorystate_fr', # code
        #'esdc_homeaddresscountryid', # code
        #'esdc_homeaddresscountry_fr', # code
        #'esdc_mailingaddressprovinceterritorystateid', # code
        #'esdc_mailingaddressprovinceterritorystate_fr', # code
        #'esdc_mailingaddresscountryid', # code
        #'esdc_mailingaddresscountry_fr', # code
        #'provincial_plan_esdc_nameid', # code
        #'federalprogram_escd_nameid', # code
        #'esdc_copaytier', # code
        #'esdc_copaytier_fr', # code
    # Structure columns:
        #'esdc_provider_details',
        #'spouse_esdc_provider_details',

    renameCols = {
    
    }
    
    @classmethod
    def _flatten_nested_column_into_new_df(cls, df, explodeCol):
        """
        Flatten a nested df by exploding col and select mapped sub-fields

        Parameters:
        keepCols 
        explodeCol
        nestedCols
        
        """

        keepCols = ['dfp_incremental_id', 'uniqueid', 'source']

        if explodeCol == 'esdc_provider_details':
            nestedCols = [
                "esdc_iscoveredraw",
                "esdc_providertype",
                "esdc_providertype_en",
                "esdc_providertype_fr",
                "esdc_employername",
                "esdc_employeraddress",
                "esdc_employerphonenumber"
            ]
        elif explodeCol == 'spouse_esdc_provider_details':
            nestedCols = [
                "esdc_iscoveredraw",
                "esdc_providertype",
                "esdc_providertype_en",
                "esdc_providertype_fr",
                "esdc_employername",
                "esdc_employeraddress",
                "esdc_employerphonenumber"
            ]

        explode_df = df.select(*keepCols, F.explode(col(explodeCol)).alias(explodeCol))

        flat_df = explode_df.select(
            *keepCols,
            *[
                col(f"{explodeCol}.{nested}")
                for nested in nestedCols
            ]
        )

        flat_df = flat_df.withColumn('insurance_details_join_id', F.concat(flat_df['dfp_incremental_id'], lit("|"), flat_df['source'])) \
            .drop('dfp_incremental_id', 'source', 'uniqueid')

        return flat_df

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df, runType, data_date):

        if runType == "all":
            df = df.transform(cls._create_string_col, newColName = 'dfp_change', string = "original")

        df = df \
            .drop('esdc_provider_details', 'spouse_esdc_provider_details') \
            .transform(cls._adjust_timestamp_from_UTC, colName = 'esdc_dateofapplication') \
            .transform(cls._adjust_timestamp_from_UTC, colName = 'esdc_attestationcompletedon') \
            .transform(cls._adjust_timestamp_from_UTC, colName = 'esdc_eligibleonfirst') \
            .transform(cls._adjust_timestamp_from_UTC, colName = 'esdc_enroledon') \
            .transform(cls._adjust_timestamp_from_UTC, colName = 'esdc_batchdate')

        df = cls._combine_duplicates_while_keeping_null_values(df, 'uniqueid')

        df = df \
            .transform(cls._replace_values, colNames = ['esdc_homeaddressprovinceterritorystate_en', 'esdc_mailingaddressprovinceterritorystate_en'], mapping = ESDCMembersWrangler.ptMapping) \
            .transform(cls._create_date_col, newColName = 'effective_date', date = data_date) \
            .transform(cls._create_date_col, newColName = 'end_date', date = "9999-12-31") \
            .transform(cls._create_time_now_col, newColName = 'created_time') \
            .transform(cls._cast_string_column_to_timestamp_then_date, colName = 'esdc_dateofapplication') \
            .transform(cls._cast_string_column_to_timestamp_then_date, colName = 'esdc_attestationcompletedon') \
            .transform(cls._cast_string_column_to_timestamp_then_date, colName = 'esdc_eligibleonfirst') \
            .transform(cls._cast_string_column_to_timestamp_then_date, colName = 'esdc_enroledon') \
            .transform(cls._cast_string_column_to_timestamp_then_date, colName = 'esdc_batchdate') \
            .transform(cls._cast_string_column_to_timestamp_then_date, colName = 'esdc_dateofbirth') \
            .transform(cls._cast_string_column_to_timestamp_then_date, colName = 'spouse_esdc_dateofbirth') \
            .transform(cls._cast_string_column_to_timestamp_then_date, colName = 'esdc_coveragestart') \
            .transform(cls._cast_string_column_to_timestamp_then_date, colName = 'esdc_coverageend') \
            .transform(cls._cast_string_column_to_date, colName = 'source') \
            .transform(cls._create_ghost_parent_column) \
            .transform(cls._create_eligible_column) \
            .transform(cls._create_enrolled_column) \
            .transform(cls._create_real_address_columns) \
            .transform(cls._create_insurance_details_join_id)

        df = df \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_esdc_members_eligible.keepCols, renameCols = wrangler_esdc_members_eligible.renameCols)

        return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Health Canada

# MARKDOWN ********************

# ### cost_of_administration

# CELL ********************

def get_read_schema_hc_cost_of_administration():
    return {
        'Date': str, # date
        'Quarter_Title': str,
        'Department': str,
        'Branch': str,
        'Funding': float,
        'Expenditure_Salary': float,
        'Expenditure_O_and_M': float
    }
    

def get_date_cols_hc_cost_of_administration():
    return [
        'Date'
    ]

def get_write_schema_hc_cost_of_administration():
    return StructType ([
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

    
class wrangler_hc_cost_of_administration(SilverWrangler):
    '''
    Class to enrich Health Canada Cost of Administration data into a standardized form.
    '''

    keepCols = [
        'Date',
        'Quarter_Title',
        'Department',
        'Branch',
        'Funding',
        'Expenditure_Salary',
        'Expenditure_O_and_M',
        'Expenditure_Total',
        'Source'
    ]

    renameCols = {
    }

    @classmethod
    def apply_transformations(cls, df):

        return df \
            .transform(cls._create_sum_column, newColName = 'Expenditure_Total', colNames = ['Expenditure_Salary', 'Expenditure_O_and_M']) \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_hc_cost_of_administration.keepCols, renameCols = wrangler_hc_cost_of_administration.renameCols)

            
registry.register_source("hc/cost_of_administration", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### cost_of_sunlife_contract

# CELL ********************

def get_read_schema_hc_cost_of_sunlife_contract():
    return {
        'Month' : str, #date
        'Contract_Budget_FY' : float,
        'Contract_Expenditure' : float,
        'Benefit_Budget_FY' : float,
        'Benefit_Expenditure' : float
    }
    

def get_date_cols_hc_cost_of_sunlife_contract():
    return [
        'Month'
    ]

def get_write_schema_hc_cost_of_sunlife_contract():
    return StructType ([
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


class wrangler_hc_cost_of_sunlife_contract(SilverWrangler):
    '''
    Class to enrich Sunlife Costing data into a standardized form.
    '''

    keepCols = [
        'FY',
        'Month',
        'Contract_Budget_FY',
        'Contract_Expenditure',
        'Contract_Expenditure_FY',
        'Contract_Budget_Remaining_FY',
        'Benefit_Budget_FY',
        'Benefit_Expenditure',
        'Benefit_Expenditure_FY',
        'Benefit_Budget_Remaining_FY'
    ]

    renameCols = {
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        return df \
            .transform(cls._create_cumsum_column_by_group, newColName = 'Contract_Expenditure_FY', sumCol = 'Contract_Expenditure', groupCol = 'FY') \
            .transform(cls._create_cumsum_column_by_group, newColName = 'Benefit_Expenditure_FY', sumCol = 'Benefit_Expenditure', groupCol = 'FY') \
            .transform(cls._subtract_num_from_total_by_group, newColName = 'Contract_Budget_Remaining_FY', totalCol = 'Contract_Budget_FY', numCol = 'Contract_Expenditure_FY', groupCol = 'FY') \
            .transform(cls._subtract_num_from_total_by_group, newColName = 'Benefit_Budget_Remaining_FY', totalCol = 'Benefit_Budget_FY', numCol = 'Benefit_Expenditure_FY', groupCol = 'FY') \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_hc_cost_of_sunlife_contract.keepCols, renameCols = wrangler_hc_cost_of_sunlife_contract.renameCols)

registry.register_source("hc/cost_of_sunlife_contract", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### eligible_population_estimates

# CELL ********************

def get_read_schema_hc_eligible_population_estimates():
    return {
        'PT': str,
        'U18': 'Int64',
        'PWD': 'Int64',
        '65 to 69': 'Int64',
        '70 to 71': 'Int64',
        '72 to 76': 'Int64',
        '77 to 86': 'Int64',
        '87+': 'Int64'
    }
    

def get_date_cols_hc_eligible_population_estimates():
    return []

def get_write_schema_hc_eligible_population_estimates():
    return StructType([
        StructField("Population", DoubleType(), True),
        StructField("PT", StringType(), True),
        StructField("Age_Group", StringType(), True),
        StructField("Latest_Age", IntegerType(), True),
        StructField("Source", DateType(), True)
    ])


class wrangler_hc_eligible_population_estimates(SilverWrangler):
    '''
    Class to enrich Eligible Population Estimates into a standardized form.
    '''

    keepCols = [
        'Eligible_Population',
        'PT',
        'Age_Group',
        'Lowest_Age',
        'Source'
    ]

    renameCols = {
        
    }


    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        df = df.loc[df['PT'] != "Total"].copy()

        df = df \
            .transform(cls._replace_value_in_column_names, newValue = "-", oldValue = "_to_")

        df = df.melt(id_vars = ['PT', 'Source'], var_name = 'Age_Group', value_name = 'Eligible_Population').sort_values(by = ['PT', 'Age_Group']).reset_index(drop=True)

        return df \
                .transform(cls._replace_values, colNames = ['Age_Group'], mapping = {"Under_18": "Under 18"}) \
                .transform(cls._create_lowest_age_column) \
                .transform(cls._sort_and_rename_cols, keepCols = wrangler_hc_eligible_population_estimates.keepCols, renameCols = wrangler_hc_eligible_population_estimates.renameCols)

                
registry.register_source("hc/eligible_population_estimates", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### procedure_codes

# CELL ********************

def get_read_schema_hc_procedure_codes():
    return {
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
    

def get_date_cols_hc_procedure_codes():
    return []

def get_write_schema_hc_procedure_codes():
    return StructType([
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


class wrangler_hc_procedure_codes(SilverWrangler):
    '''
    Class to enrich Procedure Codes into a standardized form.
    '''

    # Creating a dictionary for the postal code to province translation
    ExceptionsCSV = pd.read_csv(bronze_mount_paths['file_path'] + '/Files/ohds/mapping/Mapping_Procedure_Codes_Exceptions.csv')
    DuplicatesCSV = pd.read_csv(bronze_mount_paths['file_path'] + '/Files/ohds/mapping/Mapping_Procedure_Codes_Duplicates.csv')

    keepCols = [
        'ID',
        'Code_Combined', #rename
        'Code_Combined_Base', #rename
        'Code_Join',
        'QC_Flag',
        'Association',
        'Specialty',
        'Schedule',
        'Category_EN',
        'Category_FR',
        'Class_EN',
        'Class_FR',
        'Sub_Class_EN',
        'Sub_Class_FR',
        'Service_Title_EN',
        'Service_Title_FR',
        'Description_EN',
        'Description_FR',
        'Notes',
        'Additional_Notes',
        'Exception_Start_Date',
        'Start_Date',
        'End_Date'
    ]

    renameCols = {
        'Code_Combined': 'Code',
        'Code_Combined_Base': 'Code_Base'
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        # Filter out any row that does not have a value in any of the schedule columns
        filtered_df = df.copy() \
            .dropna(subset = ['A', 'B', 'Z', 'C', 'D'], how = 'all', ignore_index = True)

        # Create an ID column for procedures by grouping all NA descriptions with the procedure above, then fill NA descriptions with the first non-NA value above it
        filtered_df = filtered_df \
            .transform(cls._create_id_column_by_grouping_na_with_previous_value, groupColName = 'Description_EN') \
            .transform(cls._fill_na_column, oldColNames = ['Description_EN', 'Description_FR'])

        # Explode out the QC column into multiple rows by looking for the string "##### AA"
        filtered_df = cls._explode_column(filtered_df, oldColName = 'QC_Code', regexString = r'(\d{5}\s+[A-Z]{2})')
        
        # Clean whitespace and combine schedule columns into one column
        filtered_df = filtered_df \
            .transform(cls._remove_whitespace) \
            .transform(cls._clean_whitespace_between_regex_sections, oldColName = 'QC_Code_new', regexString = r'(\d{5})\s*([A-Z]{2})') \
            .transform(cls._create_column_name_column, newColName = 'Schedule', oldColNames = ['A', 'B', 'Z', 'C', 'D'])

        # When one row has a value in both Code_CA and Code_QC, split it into two rows
        filtered_df = cls._explode_codes(filtered_df)

        # Split Codes into a base code and specialty, then fix the association and specialty of certain rows.
        # Create Code_Join column from Specialty, Code, and QC_Flag. Joins to CL90 and CL92
        filtered_df = filtered_df \
            .transform(cls._expand_column, newColNames = ['Code_Combined_Base', 'Specialty'], oldColName = 'Code_Combined', numOfSplits = 1, separator = " ") \
            .transform(cls._fix_qc_association) \
            .transform(cls._fix_specialty) \
            .transform(cls._add_leading_zeros, colName = 'Code_Combined', len = 5) \
            .transform(cls._add_leading_zeros, colName = 'Code_Combined_Base', len = 5) \
            .transform(cls._create_code_join_column_procedure_codes)

        filtered_df = cls._drop_duplicates(filtered_df, subsetValue = ['Code_Join'])

        # Some QC Codes are duplicated in the ABCD document. This is to give them their actual description.
        filtered_df = filtered_df.merge(wrangler_hc_procedure_codes.DuplicatesCSV, how = 'left', left_on = 'Code_Combined', right_on = 'Duplicated_Code')

        filtered_df = filtered_df \
            .transform(cls._replace_value_in_column_A_if_column_B_not_null, columnB = 'Duplicated_Code_Description_EN', columnA = 'Description_EN') \
            .transform(cls._replace_value_in_column_A_if_column_B_not_null, columnB = 'Duplicated_Code_Description_FR', columnA = 'Description_FR')

        # Exceptions are being activated a few at a time, this signifies when the exception was turned on.
        filtered_df = filtered_df.merge(wrangler_hc_procedure_codes.ExceptionsCSV, how = 'left', left_on = 'Code_Combined', right_on = 'Exception_Code')

        filtered_df = cls._create_date_column(filtered_df, newColName = 'Start_Date', dateString = '2024-01-01')
        filtered_df = cls._create_date_column(filtered_df, newColName = 'End_Date', dateString = '2262-04-11')

        # Clean up, sort, and rename
        filtered_df = filtered_df \
            .transform(cls._cast_column_dtype, colName = 'Exception_Start_Date', newDType = "datetime64[ns]") \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_hc_procedure_codes.keepCols, renameCols = wrangler_hc_procedure_codes.renameCols)

        filtered_df = cls._sort_by_columns(filtered_df, sortingColumns = 'ID')

        return filtered_df
        

registry.register_source("hc/procedure_codes", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### sunlife_contract_milestones

# CELL ********************

def get_read_schema_hc_sunlife_contract_milestones():
    return {
        'Start_Up_Phase' : str,
        'Phase_du_Lancement' : str,
        'Milestone' : str,
        'Étape_Importante' : str,
        'Target_Date' : str, # date
        'Status' : str,
        'Statut' : str,
        'Sort' : 'Int64'
    }
    

def get_date_cols_hc_sunlife_contract_milestones():
    return [
        'Target_Date'
    ]

def get_write_schema_hc_sunlife_contract_milestones():
    return StructType ([
        StructField('Start_Up_Phase', StringType(), True),
        StructField('Phase_du_Lancement', StringType(), True),
        StructField('Milestone', StringType(), True),
        StructField('Étape_Importante', StringType(), True),
        StructField('Target_Date', DateType(), True),
        StructField('Status', StringType(), True),
        StructField('Statut', StringType(), True),
        StructField('Sort', IntegerType(), True)
    ])

    
class wrangler_hc_sunlife_contract_milestones(SilverWrangler):
    '''
    Class to enrich Sunlife Costing data into a standardized form.
    '''

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        return df

        
registry.register_source("hc/sunlife_contract_milestones", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## StatCan

# MARKDOWN ********************

# ### pccf

# CELL ********************

def get_read_schema_statcan_pccf():
    return {
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
    

def get_date_cols_statcan_pccf():
    return []

def get_write_schema_statcan_pccf():
    return StructType ([
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


class wrangler_statcan_pccf(SilverWrangler):
    '''
    Class to enrich PCCF into a standardized form.
    '''

    # Creating a dictionary for the Province Codes abbreviations
    ProvinceMapping = {
        "10": "NL",
        "11": "PE",
        "12": "NS",
        "13": "NB",
        "24": "QC",
        "35": "ON",
        "46": "MB",
        "47": "SK",
        "48": "AB",
        "59": "BC",
        "60": "YT",
        "61": "NT",
        "62": "NU"
    }


    keepCols = [
        'PostalCode',
        'FSA',
        'Area_Type',
        'PR',
        'PT_Long',
        'PT_Long_Sort',
        'PT_Group_1',
        'PT_Group_1_FR',
        'PT_Group_1_Sort',
        'PT_Group_2',
        'PT_Group_2_FR',
        'PT_Group_2_Sort',
        'PT_Group_3',
        'PT_Group_3_FR',
        'PT_Group_3_Sort',
        'CSDUid',
        'CSDName',
        'Comm_Name',
        'LAT',
        'LONG'
    ]

    renameCols = {
        'PostalCode': 'Postal_Code',
        'PR': 'PT',
        'CSDUid': 'CSD_ID',
        'CSDName': 'CSD_Name',
        'Comm_Name': 'Community_Name',
        'LAT': 'Latitude',
        'LONG': 'Longitude'
    }

    @classmethod
    def _create_FSA_table(cls, df):
        
        df_fsa = df.drop_duplicates(subset = ['FSA']).drop(columns = ['Postal_Code'])

        return df_fsa

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        filtered_df = df.query("SLI == '1'").copy()

        return filtered_df \
            .transform(cls._replace_values, colNames = ['PR'], mapping = wrangler_statcan_pccf.ProvinceMapping) \
            .transform(cls._create_area_type_column, newColName = 'Area_Type', colName = 'PostalCode') \
            .transform(cls._add_characters, colName = 'PostalCode', character = " ", positions = [3])

    
    @classmethod
    def apply_joins(cls, df):

        # Read in the dataframes to be joined
        df_pt = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.grouping.grouping_pt').toPandas()

        # Join the dataframes
        df_merged = df.merge(df_pt, how = 'left', left_on = 'PR', right_on = 'Abbreviation')
        
        # Sort and rename the columns in PCCF
        df_merged = df_merged \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_statcan_pccf.keepCols, renameCols = wrangler_statcan_pccf.renameCols)

        return df_merged

        
registry.register_source("statcan/pccf", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sunlife

# MARKDOWN ********************

# ### cards_mailout

# CELL ********************

def get_read_schema_sunlife_cards_mailout():
    return {
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
    

def get_date_cols_sunlife_cards_mailout():
    return [
        'Month'
    ]

def get_write_schema_sunlife_cards_mailout():
    return StructType ([
        StructField('Month', DateType(), True),
        StructField('Type', StringType(), True),
        StructField('Count', DoubleType(), True)
    ])

    
class wrangler_sunlife_cards_mailout(SilverWrangler):
    '''
    Class to enrich SunLife Cards into a standardized form.
    '''

    keepCols = [
        'Month',
        'Type',
        'Count'
    ]

    renameCols = {
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        df = df.drop(columns = ['Source'])

        df = df.melt(id_vars = ['Month'], var_name = 'Type', value_name = 'Count')

        return df \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_sunlife_cards_mailout.keepCols, renameCols = wrangler_sunlife_cards_mailout.renameCols)

            
registry.register_source("sunlife/cards_mailout", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### cl90

# CELL ********************

def get_read_schema_sunlife_cl90():
    return {
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
    

def get_date_cols_sunlife_cl90():
    return [
        'Service Date',
        'Submitted Date',
        'Adjudicated Date',
        'Amended Date',
        'Analysis Date '
    ]

def get_write_schema_sunlife_cl90():
    return StructType([  
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

    
class wrangler_sunlife_cl90(SilverWrangler):
    '''
    Class to enrich CL90 into a standardized form.
    '''
    keepCols = [
        'Claim_Reference_Number',
        'Member_ID', 
        'Member_Province',
        'Member_PT_Alt',
        'Member_Postal_Code',
        'Member_FSA',
        'Age',
        'Provider_ID',
        'Provider_Facility_ID',
        'QC_Flag',
        'Provider_Province',
        'Facility_PT_Alt',
        'Provider_Postal_Code',
        'Facility_FSA',
        'Provider_Participation_Type',
        'Specialty',
        'Co-Pay(Plan_ID)',
        'Benefit_Category',
        'Procedure_Code',
        'Code_Join_Submitted',
        'SL_Procedure_Submitted_Description_EN',
        'SL_Procedure_Submitted_Description_FR',
        'Procedure_Code_Paid',
        'Code_Join_Paid',
        'SL_Procedure_Paid_Description_EN',
        'SL_Procedure_Paid_Description_FR',
        'Tooth_Number',
        'Tooth_Surface',
        'Post-Determination_Indicator',
        'Schedule',
        'Service_Date',
        'Claim_Date',
        'Submitted_Date',
        'Adjudicated_Date',
        'Amended_Date',
        'Analysis_Date_',
        'Paper_Claim_Indicator',
        'Reason_Code',
        'Reason_Description',
        'Remark_Code',
        'Remark_Description',
        'Cheque_Number',
        'Payment_Method',
        'Coordination_of_Benefits_Indicator',
        'Submitted_Amount',
        'Eligible_Amount',
        'Coordination_of_Benefits_Amount',
        'Paid_Amount',
        'Source'
    ]

    renameCols = {
        'Member_Province': 'Member_PT',
        'Provider_Facility_ID': 'Facility_ID',
        'Provider_Province': 'Facility_PT',
        'Provider_Postal_Code': 'Facility_Postal_Code',
        'Procedure_Code': 'Procedure_Code_Submitted',
        'Co-Pay(Plan_ID)': 'Co_Pay_Tier',
        'Post-Determination_Indicator': 'Post_Determination_Indicator',
        'Coordination_of_Benefits_Indicator': 'COB_Indicator',
        'Coordination_of_Benefits_Amount': 'COB_Amount',
        'Analysis_Date_': 'Analysis_Date'
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        df_os_codes = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.hc.hc_procedure_codes_os').toPandas()

        # Transformations
        df = df \
            .transform(cls._remove_whitespace) \
            .transform(cls._replace_values, colNames = ['Provider_Province'], mapping = {"NF": "NL"}) \
            .transform(cls._replace_values, colNames = ['Provider_Participation_Type'], mapping = {"Y": "Participating Provider", "N": "Claim-by-Claim Provider", "S": "Mixed Service Dates", "?": "Unknown", "D": "Do Not Enrol"}) \
            .transform(cls._replace_values, colNames = ['Provider_ID', 'Provider_Facility_ID', 'Procedure_Code']) \
            .transform(cls._add_leading_zeros, colName = 'Provider_ID', len = 9) \
            .transform(cls._add_leading_zeros, colName = 'Provider_Facility_ID', len = 9) \
            .transform(cls._add_leading_zeros, colName = 'Procedure_Code', len = 5) \
            .transform(cls._add_leading_zeros, colName = 'Procedure_Code_Paid', len = 5) \
            .transform(cls._cast_yes_no_to_bool, colName = 'Coordination_of_Benefits_Indicator') \
            .transform(cls._create_claim_date_column) \
            .transform(cls._create_province_column_from_postal_code, newColName = 'Member_PT_Alt', colName = 'Member_Postal_Code', mapping = super().PostalCodeMapping) \
            .transform(cls._create_province_column_from_postal_code, newColName = 'Facility_PT_Alt', colName = 'Provider_Postal_Code', mapping = super().PostalCodeMapping) \
            .transform(cls._create_FSA_column, newColName = 'Member_FSA', colName = 'Member_Postal_Code') \
            .transform(cls._create_FSA_column, newColName = 'Facility_FSA', colName = 'Provider_Postal_Code') \
            .transform(cls._create_qc_flag_cl90)

        df = df.merge(df_os_codes, how = 'left', left_on = 'Procedure_Code', right_on = 'Code_Base', suffixes = ['', '_Submitted'])
        df = df.merge(df_os_codes, how = 'left', left_on = 'Procedure_Code_Paid', right_on = 'Code_Base', suffixes = ['_Submitted', '_Paid'])

        return df \
            .transform(cls._create_os_flag_cl90, newColName = 'OS_Flag_Submitted', oldColName = 'Code_Base_Submitted') \
            .transform(cls._create_os_flag_cl90, newColName = 'OS_Flag_Paid', oldColName = 'Code_Base_Paid') \
            .transform(cls._create_code_join_cl90, newColName = 'Code_Join_Submitted', procedureCodeCol = 'Procedure_Code', OSFlagCol = 'OS_Flag_Submitted') \
            .transform(cls._create_code_join_cl90, newColName = 'Code_Join_Paid', procedureCodeCol = 'Procedure_Code_Paid', OSFlagCol = 'OS_Flag_Paid')


    @classmethod
    def apply_joins(cls, df):

        # Read in the dataframes to be joined
        df_procedure_codes_submitted = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.sunlife.sunlife_procedure_code_descriptions').toPandas()
        df_procedure_codes_paid = df_procedure_codes_submitted.copy()
        df_reason_codes = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.sunlife.sunlife_reason_and_remark_codes').toPandas()
        df_remark_codes = df_reason_codes.copy()

        # Join Procedure_Code_Descriptions on the column "Procedure_Code_Submitted"
        procedure_code_submitted_columns = ['SL_Procedure_Code_Submitted', 'SL_Procedure_Submitted_Description_EN', 'SL_Procedure_Submitted_Description_FR']
        df_procedure_codes_submitted.columns = procedure_code_submitted_columns
        df_merged = df.merge(df_procedure_codes_submitted, how = 'left', left_on = 'Procedure_Code', right_on = 'SL_Procedure_Code_Submitted')

        # Join Procedure_Code_Descriptions on the column "Procedure_Code_Paid"
        procedure_code_paid_columns = ['SL_Procedure_Code_Paid', 'SL_Procedure_Paid_Description_EN', 'SL_Procedure_Paid_Description_FR']
        df_procedure_codes_paid.columns = procedure_code_paid_columns
        df_merged = df_merged.merge(df_procedure_codes_paid, how = 'left', left_on = 'Procedure_Code_Paid', right_on = 'SL_Procedure_Code_Paid')
      
        # Join Reason_and_Remark_Codes on the column "Reason_Code"
        df_merged = df_merged.merge(df_reason_codes, how = 'left', on = 'Reason_Code')

        # Join Reason_and_Remark_Codes on the column "Remark_Code"
        remark_code_columns = ['Remark_Code', 'Remark_Description']
        df_remark_codes.columns = remark_code_columns
        df_merged = df_merged.merge(df_remark_codes, how = 'left', on = 'Remark_Code')

        # Sort and rename the columns in CL90
        df_merged = df_merged \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_sunlife_cl90.keepCols, renameCols = wrangler_sunlife_cl90.renameCols)

        return df_merged


registry.register_source("sunlife/cl90", "append")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### cl92

# CELL ********************

def get_read_schema_sunlife_cl92():
    return {
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
    

def get_date_cols_sunlife_cl92():
    return [
        'Service Date',
        'Submitted Date',
        'Adjudicated Date',
        'Analysis Date '
    ]

def get_write_schema_sunlife_cl92():
    return StructType([  
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

    
class wrangler_sunlife_cl92(SilverWrangler):
    '''
    Class to enrich CL92 into a standardized form.
    '''
    keepCols = [
        'Claim_Reference_Number',
        'Member_ID', 
        'Member_Province',
        'Member_PT_Alt',
        'Member_Postal_Code',
        'Member_FSA',
        'Age',
        'Provider_ID',
        'Provider_Facility_ID',
        'QC_Flag',
        'Provider_Province',
        'Facility_PT_Alt',
        'Provider_Postal_Code',
        'Facility_FSA',
        'Provider_Participation_Type',
        'Specialty',
        'Co-Pay(Plan_ID)',
        'Benefit_Category',
        'Procedure_Code',
        'Code_Join_Submitted',
        'SL_Procedure_Submitted_Description_EN',
        'SL_Procedure_Submitted_Description_FR',
        'Procedure_Code_Paid',
        'Code_Join_Paid',
        'SL_Procedure_Paid_Description_EN',
        'SL_Procedure_Paid_Description_FR',
        'Tooth_Number',
        'Tooth_Surface',
        'Preauthorization_Indicator',
        'Schedule',
        'Service_Date',
        'Claim_Date',
        'Submitted_Date',
        'Adjudicated_Date',
        'Analysis_Date_',
        'Paper_Claim_Indicator',
        'Reason_Code',
        'Reason_Description',
        'Remark_Code',
        'Remark_Description',
        'Submitted_Amount',
        'Eligible_Amount',
        'Paid_Amount',
        'Source'
    ]

    renameCols = {
        'Member_Province': 'Member_PT',
        'Provider_Facility_ID': 'Facility_ID',
        'Provider_Province': 'Facility_PT',
        'Provider_Postal_Code': 'Facility_Postal_Code',
        'Co-Pay(Plan_ID)': 'Co_Pay_Tier',
        'Procedure_Code': 'Procedure_Code_Submitted',
        'Analysis_Date_': 'Analysis_Date'
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        df_os_codes = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.hc.hc_procedure_codes_os').toPandas()

        # Transformations
        df = df \
            .transform(cls._remove_whitespace) \
            .transform(cls._replace_values, colNames = ['Provider_Province'], mapping = {"NF": "NL"}) \
            .transform(cls._replace_values, colNames = ['Provider_ID', 'Provider_Facility_ID', 'Procedure_Code']) \
            .transform(cls._add_leading_zeros, colName = 'Provider_ID', len = 9) \
            .transform(cls._add_leading_zeros, colName = 'Provider_Facility_ID', len = 9) \
            .transform(cls._add_leading_zeros, colName = 'Procedure_Code', len = 5) \
            .transform(cls._add_leading_zeros, colName = 'Procedure_Code_Paid', len = 5) \
            .transform(cls._create_claim_date_column) \
            .transform(cls._create_province_column_from_postal_code, newColName = 'Member_PT_Alt', colName = 'Member_Postal_Code', mapping = super().PostalCodeMapping) \
            .transform(cls._create_province_column_from_postal_code, newColName = 'Facility_PT_Alt', colName = 'Provider_Postal_Code', mapping = super().PostalCodeMapping) \
            .transform(cls._create_FSA_column, newColName = 'Member_FSA', colName = 'Member_Postal_Code') \
            .transform(cls._create_FSA_column, newColName = 'Facility_FSA', colName = 'Provider_Postal_Code') \
            .transform(cls._create_qc_flag_cl90)

        df = df.merge(df_os_codes, how = 'left', left_on = 'Procedure_Code', right_on = 'Code_Base', suffixes = ['', '_Submitted'])
        df = df.merge(df_os_codes, how = 'left', left_on = 'Procedure_Code_Paid', right_on = 'Code_Base', suffixes = ['_Submitted', '_Paid'])

        return df \
            .transform(cls._create_os_flag_cl90, newColName = 'OS_Flag_Submitted', oldColName = 'Code_Base_Submitted') \
            .transform(cls._create_os_flag_cl90, newColName = 'OS_Flag_Paid', oldColName = 'Code_Base_Paid') \
            .transform(cls._create_code_join_cl90, newColName = 'Code_Join_Submitted', procedureCodeCol = 'Procedure_Code', OSFlagCol = 'OS_Flag_Submitted') \
            .transform(cls._create_code_join_cl90, newColName = 'Code_Join_Paid', procedureCodeCol = 'Procedure_Code_Paid', OSFlagCol = 'OS_Flag_Paid')

    @classmethod
    def apply_joins(cls, df):

        # Read in the dataframes to be joined
        df_procedure_codes_submitted = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.sunlife.sunlife_procedure_code_descriptions').toPandas()
        df_procedure_codes_paid = df_procedure_codes_submitted.copy()
        df_reason_codes = spark.sql(f'SELECT * FROM {silver_lakehouse_name}.sunlife.sunlife_reason_and_remark_codes').toPandas()
        df_remark_codes = df_reason_codes.copy()

        # Join Procedure_Code_Descriptions on the column "Procedure_Code_Submitted"
        procedure_code_submitted_columns = ["SL_Procedure_Code_Submitted", "SL_Procedure_Submitted_Description_EN", "SL_Procedure_Submitted_Description_FR"]
        df_procedure_codes_submitted.columns = procedure_code_submitted_columns
        df_merged = df.merge(df_procedure_codes_submitted, how = 'left', left_on = 'Procedure_Code', right_on = 'SL_Procedure_Code_Submitted')

        # Join Procedure_Code_Descriptions on the column "Procedure_Code_Paid"
        procedure_code_paid_columns = ["SL_Procedure_Code_Paid", "SL_Procedure_Paid_Description_EN", "SL_Procedure_Paid_Description_FR"]
        df_procedure_codes_paid.columns = procedure_code_paid_columns
        df_merged = df_merged.merge(df_procedure_codes_paid, how = 'left', left_on = 'Procedure_Code_Paid', right_on = 'SL_Procedure_Code_Paid')
      
        # Join Reason_and_Remark_Codes on the column "Reason_Code"
        df_merged = df_merged.merge(df_reason_codes, how = 'left', on = 'Reason_Code')

        # Join Reason_and_Remark_Codes on the column "Remark_Code"
        remark_code_columns = ['Remark_Code', 'Remark_Description']
        df_remark_codes.columns = remark_code_columns
        df_merged = df_merged.merge(df_remark_codes, how = 'left', on = 'Remark_Code')

        # Sort and rename the columns in CL92
        df_merged = df_merged \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_sunlife_cl92.keepCols, renameCols = wrangler_sunlife_cl92.renameCols)

        return df_merged

        
registry.register_source("sunlife/cl92", "append")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### contact_centre_stats

# CELL ********************

def get_read_schema_sunlife_contact_centre_stats():
    return {
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
    

def get_date_cols_sunlife_contact_centre_stats():
    return [
        'Contact_Date'
    ]

def get_write_schema_sunlife_contact_centre_stats():
    return StructType([
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

    
class wrangler_sunlife_contact_centre_stats(SilverWrangler):
    '''
    Class to enrich Contact Center Metrics into a standardized form.
    '''

    keepCols = [
        'Queue_Category',
        'Language',
        'Language_FR',
        'Contact_Date',
        'Service_Level',
        'Average_Handle_Time',
        'Average_Handle_Time_Mins',
        'Average_Wait_Time',
        'Contacts_Abandoned',
        'Contacts_Handled_Incoming',
        'Callback_Contacts_Handled',
        'Total_Calls',
        'Source'
    ]

    renameCols = {
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        return df \
            .transform(cls._create_fr_column, newColName = 'Language_FR', oldColName = 'Language', mapping = {'English': 'Anglais', 'French': 'Français'}) \
            .transform(cls._convert_seconds_to_minutes, newColName = 'Average_Handle_Time_Mins', colName = 'Average_Handle_Time') \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_sunlife_contact_centre_stats.keepCols, renameCols = wrangler_sunlife_contact_centre_stats.renameCols)

            
registry.register_source("sunlife/contact_centre_stats", "append")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fi02

# CELL ********************

def get_read_schema_sunlife_fi02():
    return {
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
    

def get_date_cols_sunlife_fi02():
    return [
        'CFR Date',
        'Adjudicated Date'
    ]

def get_write_schema_sunlife_fi02():
    return StructType([ 
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

    
class wrangler_sunlife_fi02(SilverWrangler):
    '''
    Class to enrich FI02 into a standardized form.
    '''
    
    keepCols = [
        'Provider_Number',
        'Provider_Facility_ID',
        'Member_ID',
        'Claim_Reference_Number',
        'Cheque_Number',
        'Billing_Group',
        'Member_Province/Territories',
        'Member_PT_Alt',
        'Member_Postal_Code',
        'Member_FSA',
        'Provider_Province/Territories',
        'Facility_PT_Alt',
        'Provider_Postal_Code',
        'Facility_FSA',
        'CFR_Date',
        'Claim_Date',
        'Adjudicated_Date',
        'EFT',
        'Cheques',
        'Payment_Cancellations_EFT',
        'Payment_Cancellations_Cheque',
        'Refunds_EFT',
        'Refunds_Cheque',
        'Other_Adjustments',
        'Voids_EFT',
        'Voids_Cheque',
        'Total_Amount',
        'Source'
    ]

    renameCols = {
        'Provider_Number': 'Provider_ID',
        'Provider_Facility_ID': 'Facility_ID',
        'Member_Province/Territories': 'Member_PT',
        'Provider_Province/Territories': 'Facility_PT',
        'Provider_Postal_Code': 'Facility_Postal_Code'
    }


    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        return df \
            .transform(cls._add_leading_zeros, colName = 'Provider_Number', len = 9) \
            .transform(cls._add_leading_zeros, colName = 'Provider_Facility_ID', len = 9) \
            .transform(cls._add_leading_zeros, colName = 'Billing_Group', len = 3) \
            .transform(cls._create_sum_column, newColName = 'Total_Amount', colNames = ['EFT', 'Cheques', 'Payment_Cancellations_EFT', 'Payment_Cancellations_Cheque', 'Refunds_EFT', 'Refunds_Cheque', 'Voids_EFT', 'Voids_Cheque', 'Other_Adjustments']) \
            .transform(cls._create_claim_date_column) \
            .transform(cls._create_province_column_from_postal_code, newColName = 'Member_PT_Alt', colName = 'Member_Postal_Code', mapping = super().PostalCodeMapping) \
            .transform(cls._create_province_column_from_postal_code, newColName = 'Facility_PT_Alt', colName = 'Provider_Postal_Code', mapping = super().PostalCodeMapping) \
            .transform(cls._create_FSA_column, newColName = 'Member_FSA', colName = 'Member_Postal_Code') \
            .transform(cls._create_FSA_column, newColName = 'Facility_FSA', colName = 'Provider_Postal_Code') \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_sunlife_fi02.keepCols, renameCols = wrangler_sunlife_fi02.renameCols)

            
registry.register_source("sunlife/fi02", "append")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### pp08

# CELL ********************

def get_read_schema_sunlife_pp08():
    return {
        'Provider ID': str, 
        'PT': str, 
        'Provider Postal Code': str, 
        'Participating Date': str, # date 
        'Specialty': str,
        'Language': str,
        'Direct Billing': str
    }
    

def get_date_cols_sunlife_pp08():
    return [
        'Participating Date'
    ]

def get_write_schema_sunlife_pp08():
    return StructType([ 
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

    
class wrangler_sunlife_pp08(SilverWrangler):
    '''
    Class to enrich PP08 into a standardized form.
    '''
    
    # Creating a dictionary for the Specialty abbreviations
    SpecialtyCSV = pd.read_csv(bronze_mount_paths['file_path'] + '/Files/ohds/mapping/Mapping_Specialty.csv')
    SpecialtyMapping = SpecialtyCSV.set_index('PP08_Specialty')['Abbreviation'].to_dict()

    keepCols = [
        'Provider_ID',
        'Province',
        'Provider_PT_Alt',
        'Postal_Code',
        'Provider_FSA',
        'Provider_Area_Type',
        'Participating_Date',
        'Enrolled_Status',
        'Specialty',
        'Language',
        'Direct_Billing',
        'Source'
    ]

    renameCols = {
        'Province': 'Provider_PT',
        'Postal_Code': 'Provider_Postal_Code'
    }


    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        cls._create_enrolled_status_column(df)

        return df \
            .transform(cls._replace_values, colNames = ['Specialty'], mapping = wrangler_sunlife_pp08.SpecialtyMapping) \
            .transform(cls._add_leading_zeros, colName = 'Provider_ID', len = 9) \
            .transform(cls._cast_yes_no_to_bool, colName = 'Direct_Billing') \
            .transform(cls._create_province_column_from_postal_code, newColName = 'Provider_PT_Alt', colName = 'Postal_Code', mapping = super().PostalCodeMapping) \
            .transform(cls._create_FSA_column, newColName = 'Provider_FSA', colName = 'Postal_Code') \
            .transform(cls._create_area_type_column, newColName = 'Provider_Area_Type', colName = 'Postal_Code') \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_sunlife_pp08.keepCols, renameCols = wrangler_sunlife_pp08.renameCols)

            
registry.register_source("sunlife/pp08", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### procedure_code_descriptions

# CELL ********************

def get_read_schema_sunlife_procedure_code_descriptions():
    return {
        'SL_Procedure_Code': str,
        'SL_Procedure_Description_EN': str,
        'SL_Procedure_Description_FR': str
    }
    

def get_date_cols_sunlife_procedure_code_descriptions():
    return []

def get_write_schema_sunlife_procedure_code_descriptions():
    return StructType([
        StructField("SL_Procedure_Code", StringType(), True),
        StructField("SL_Procedure_Description_EN", StringType(), True),
        StructField("SL_Procedure_Description_FR", StringType(), True)
    ])

    
class wrangler_sunlife_procedure_code_descriptions(SilverWrangler):
    '''
    Class to enrich ESDC Enrolled PT into a standardized form.
    '''

    keepCols = [
        'SL_Procedure_Code',
        'SL_Procedure_Description_EN',
        'SL_Procedure_Description_FR'
    ]

    renameCols = {
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        return df \
            .transform(cls._add_leading_zeros, colName = 'SL_Procedure_Code', len = 5) \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_sunlife_procedure_code_descriptions.keepCols, renameCols = wrangler_sunlife_procedure_code_descriptions.renameCols)

            
registry.register_source("sunlife/procedure_code_descriptions", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### provider_billing

# CELL ********************

def get_read_schema_sunlife_provider_billing():
    return {
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
    

def get_date_cols_sunlife_provider_billing():
    return [
        'Date'
    ]

def get_write_schema_sunlife_provider_billing():
    return StructType ([
        StructField('Date', DateType(), True),
        StructField('Geo_Type', StringType(), True),
        StructField('Count_Type', StringType(), True),
        StructField('PT', StringType(), True),
        StructField('FSA', StringType(), True),
        StructField('Specialty', StringType(), True),
        StructField('Direct_Billing', StringType(), True),
        StructField('Count', IntegerType(), True)
    ])

    
class wrangler_sunlife_provider_billing(SilverWrangler):
    '''
    Class to enrich Sunlife Costing data into a standardized form.
    '''

    keepCols = [
        'Date',
        'Geo_Type',
        'Count_Type',
        'PT',
        'FSA',
        'Specialty',
        'Direct_Billing',
        'Count'
    ]

    renameCols = {
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        df = df.drop(columns = ['TOTAL_Direct', 'TOTAL_Not_Direct'])

        df = df.melt(id_vars = ['Date', 'Geo_Type', 'Count_Type', 'PT', 'FSA'], value_name = 'Count')

        return df \
            .transform(cls._replace_values, colNames = ['Count'], mapping = {None: '0'}) \
            .transform(cls._cast_null_strings_to_int, colName = 'Count') \
            .transform(cls._expand_column, newColNames = ['Specialty', 'Direct_Billing'], oldColName = 'variable', numOfSplits = 1, separator = "_") \
            .transform(cls._replace_values, colNames = ['Direct_Billing'], mapping = {"Direct": "Yes", "Non_Direct": "No"}) \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_sunlife_provider_billing.keepCols, renameCols = wrangler_sunlife_provider_billing.renameCols)

            
registry.register_source("sunlife/provider_billing", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### reason_and_remark_codes

# CELL ********************

def get_read_schema_sunlife_reason_and_remark_codes():
    return {
        'Reason_Code': str,
        'Reason_Description': str,
    }
    

def get_date_cols_sunlife_reason_and_remark_codes():
    return []

def get_write_schema_sunlife_reason_and_remark_codes():
    return StructType([
        StructField("Reason_Code", StringType(), True),
        StructField("Reason_Description", StringType(), True)
    ])

    
class wrangler_sunlife_reason_and_remark_codes(SilverWrangler):
    '''
    Class to enrich Reason and Remark Codes into a standardized form.
    '''

    keepCols = [
        'Reason_Code',
        'Reason_Description',
        'Source'
    ]

    renameCols = {
    }

    # When this class is called, it will apply all the following transformations to the dataframe.
    @classmethod
    def apply_transformations(cls, df):

        df = df.drop_duplicates(subset = ['Reason_Code'])

        return df \
            .transform(cls._sort_and_rename_cols, keepCols = wrangler_sunlife_reason_and_remark_codes.keepCols, renameCols = wrangler_sunlife_reason_and_remark_codes.renameCols)

            
registry.register_source("sunlife/reason_and_remark_codes", "overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Bronze to Silver Variable Assignment

# CELL ********************

#
sourceType = "csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#print(config["read_schema"])
#print(config["read_schema"]())

#print(config["date_cols"])
#print(config["date_cols"]())

#print(config["write_schema"])
#print(config["write_schema"]())

#print(config["write_path"])
#print(config["write_path"]())

#print(config["write_behaviour"])
#print(config["apply_transformations"])
#print(config["apply_joins"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
