# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# ###### <span style="font-size: 26px; font-family: san-serif;">**Unit Test Example**</span><br/>  
# <br/>
# <span style="font-size: 15px; font-family: Garamond, san-serif;">Robby Bemrose</span> <br/>
# <span style="font-size: 15px; font-family: Garamond, san-serif;">OHDS</span> <br/>
# <span style="font-size: 14px; font-family: Garamond, san-serif;">November 2024</span> <br/>

# MARKDOWN ********************

# <span style="font-size: 24px; font-family: Garamond, san-serif;">Postal Code Check Function</span><br/>  

# CELL ********************

import pandas as pd
import re

df = pd.DataFrame({'Member_Postal_Code':['K0A 2W0','K1S 2C4','Z6K 3E9','K1A 0T6','Q4W 4X6','K0A Q4T']})

def ck_pcode(pcode):
    """
    """
    list0 = ['A', 'B', 'C', 'E', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'V', 'X', 'Y']
    list1 = ['A', 'B', 'C', 'E', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'T', 'V', 'W', 'X', 'Y', 'Z']
    pattern = rf'^[{"".join(list0)}]\d[{"".join(list1)}] \d[{"".join(list1)}]\d$'
    return re.fullmatch(pattern, pcode) is not None

df['valid_flg'] = df['Member_Postal_Code'].apply(ck_pcode)
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <span style="font-size: 24px; font-family: Garamond, san-serif;">Unit Test Example</span><br/>  


# CELL ********************

import unittest
import pandas as pd
import logging
import sys

# superclass
class SuperC:
    @staticmethod
    def remove(df):
        # select cols with string dtypes and remove whitespace
        col_sel = df.select_dtypes(include = ['object']).columns
        df[col_sel] = df[col_sel].apply(lambda x: x.str.strip())
        return df

# subclass
class CL90(SuperC):
    @classmethod
    def apply_transformations(cls, df):
        df = cls.remove(df)
        return df

df = pd.DataFrame({
            'Provider_Facility_ID': ['   000376033','000376033   '],
            'Provider_ID': ['000822786  ',' 00084786'],
            'Procedure_Code': [' 31310','31320    '],
            'Provider_PT' : ['NL','NF'],
            'ETF': [125, 20]
        })

# unit tests
class TestTransformed(unittest.TestCase):

    def setUp(self):
        self.df = df.copy()

    def test_remove_whitespace(self):
        """
        do any of the str cols have leading or trailing whitespaces
        """
        out_df = CL90.apply_transformations(self.df)
        
        sel_cols = out_df.select_dtypes(include = 'object')
        for col in sel_cols.columns:
            self.assertFalse(out_df[col].str.startswith(' ').any(), msg = f"leading whitespace in {col}")
            self.assertFalse(out_df[col].str.endswith(' ').any(), msg = f"trailing whitespace in {col}")

if __name__ == '__main__':
    unittest.main(argv = ['first-arg-is-ignored'], exit = False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
