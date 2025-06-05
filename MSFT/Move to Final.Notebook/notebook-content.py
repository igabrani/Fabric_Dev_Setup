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
# META     }
# META   }
# META }

# CELL ********************

import os
import shutil
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

provider=''
parent_folder=''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temp_root=''
final_root=''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if parent_folder == 'main':
    temp_root="/lakehouse/default/Files/main/"+provider+"/in/temp"
    final_root="/lakehouse/default/Files/main/"+provider+"/in"
elif parent_folder == 'outbox_log':
    temp_root="/lakehouse/default/Files/main/"+provider+"/out"
    final_root="/lakehouse/default/Files/outbox_log/"+provider+"/out"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(temp_root)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(final_root)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for root, dirs, files in os.walk(temp_root):
    for file in files:
        original_path = os.path.join(root, file)
        
        # Extract file name and extension
        file_extension = file.split('.')[-1] if '.' in file else ''
        base_name = file.rsplit('.', 1)[0] if '.' in file else file  # Handle files with no extension
        
        # Append timestamp to file name
        new_name = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S%f')}.{file_extension}" if file_extension else f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S%f')}"
        
        # Maintain folder structure
        new_path = original_path.replace(temp_root, final_root).replace(file, new_name)

        # Move and ensure deletion
        shutil.move(original_path, new_path)
        if os.path.exists(original_path):
            os.remove(original_path)

        print(f"Moved and Renamed: {original_path} -> {new_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
