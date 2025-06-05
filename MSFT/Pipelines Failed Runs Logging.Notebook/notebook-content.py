# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4c00abc9-207b-4af4-a90f-8c0fb835d776",
# META       "default_lakehouse_name": "Files_Lakehouse",
# META       "default_lakehouse_workspace_id": "4748fbe5-9b18-4aac-9d74-f79c39ff81db",
# META       "known_lakehouses": []
# META     },
# META     "warehouse": {}
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
!pip install /lakehouse/default/Files/module/fabric_sdk-0.1.1-py3-none-any.whl

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from fabric_sdk.api import FabricAPIBase

# Create an instance and use the methods
fabric_api = FabricAPIBase()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

Pipeline_name=''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace = notebookutils.runtime.context.get("currentWorkspaceName")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric_api.GetPipelineLastRunStatus(workspace,Pipeline_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

status=fabric_api.GetPipelineLastRunStatus(workspace,Pipeline_name)['status'][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


date_time=fabric_api.GetPipelineLastRunStatus(workspace,Pipeline_name)['startTimeUtc'][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Result={"PipelineName":Pipeline_name,"RunStatus":status,"RunDate":date_time}
mssparkutils.notebook.exit(str(Result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
