# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4c00abc9-207b-4af4-a90f-8c0fb835d776",
# META       "default_lakehouse_name": "Files_Lakehouse",
# META       "default_lakehouse_workspace_id": "4748fbe5-9b18-4aac-9d74-f79c39ff81db",
# META       "known_lakehouses": [
# META         {
# META           "id": "4c00abc9-207b-4af4-a90f-8c0fb835d776"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

provider=''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

in_path = f"/lakehouse/default/Files/main/{provider}/in/temp"
# Create the folder
os.makedirs(in_path, exist_ok=True)

print("Folder created:", in_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

out_path = f"/lakehouse/default/Files/main/{provider}/out"

os.makedirs(out_path, exist_ok=True)

print("Folder created:", out_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

out_log_path=f"/lakehouse/default/Files/outbox_log/{provider}/out"

os.makedirs(out_log_path, exist_ok=True)

print("Folder created:", out_log_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
