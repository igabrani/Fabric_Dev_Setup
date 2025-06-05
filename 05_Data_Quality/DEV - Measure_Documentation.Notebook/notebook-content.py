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

# CELL ********************

import sempy.fabric as fabric
from sempy.relationships import plot_relationship_metadata
from sempy.relationships import find_relationships
from sempy.fabric import list_relationship_violations

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Datasets

# CELL ********************

fabric.list_datasets()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Measures

# CELL ********************

dataset = "CDCP Data Model"
#fabric.list_measures(dataset)
measures = fabric.list_measures(dataset)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(measures)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test = measures.loc[measures['Measure Name'] == "test4", 'Measure Display Folder'].values[0]
#test = measures.loc[measures['Measure Name'] == "test4", 'Measure Name'].values[0]
display(len(test))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

measures.to_csv("abfss://CDCP@onelake.dfs.fabric.microsoft.com/Data_Quality_Lakehouse_PROD.Lakehouse/Files/Dictionary/Measures.csv", index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Measure Example

# CELL ********************

fabric.evaluate_measure(dataset, measure="FI02_Distinct_Claims_Member_PT", groupby_columns=["grouping_pt[PT_Group_1]"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Relationships

# CELL ********************

relationships = fabric.list_relationships(dataset)
display(relationships)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

list_relationship_violations(tables, fabric.list_relationships(dataset))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plot_relationship_metadata(relationships)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Tables

# CELL ********************

fabric.list_tables(dataset)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table = fabric.read_table(dataset, "sunlife_cards_mailout")
display(table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

suggested_relationships_all = find_relationships(
    tables,
    name_similarity_threshold=0.7,
    coverage_threshold=0.7,
    verbose=2
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
