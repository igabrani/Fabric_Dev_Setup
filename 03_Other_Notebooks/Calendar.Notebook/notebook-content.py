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

import pandas as pd
import calendar
from datetime import datetime

def build_workweek_calendar(start: str, end: str) -> pd.DataFrame:
    # Create date range of business days (Mon–Fri)
    dates = pd.date_range(start=start, end=end, freq='B')

    # Create base DataFrame
    df = pd.DataFrame({'Date': dates})
    
    # Calculate start of the week (Monday)
    df['Week Start'] = df['Date'] - pd.to_timedelta(df['Date'].dt.weekday, unit='D')
    
    # Use the month of the Monday (week start) for Month-Year
    df['Month-Year'] = df['Week Start'].dt.strftime('%b-%y')
    
    # Extract day names and day numbers
    df['Day Name'] = df['Date'].dt.strftime('%a')
    df['Day Num'] = df['Date'].dt.day

    # Pivot to wide format: one row per week, Mon-Fri as columns
    calendar_df = df.pivot_table(index=['Month-Year', 'Week Start'],
                                 columns='Day Name',
                                 values='Day Num',
                                 aggfunc='first')

    # Ensure correct weekday order
    weekday_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
    calendar_df = calendar_df.reindex(columns=weekday_order)

    # Sort by week start
    calendar_df = calendar_df.sort_index(level='Week Start').reset_index()

    return calendar_df



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example usage
calendar_df = build_workweek_calendar("2025-06-01", "2026-03-31")
display(calendar_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

calendar_df.to_csv("abfss://4748fbe5-9b18-4aac-9d74-f79c39ff81db@onelake.dfs.fabric.microsoft.com/9b0ffb12-995f-4c54-8fec-80e863a21b73/Files/calendar.csv", index = False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
