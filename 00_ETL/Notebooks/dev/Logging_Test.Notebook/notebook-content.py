# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

!pip install --force-reinstall \
    "azure-kusto-data==4.2.0" \
    "azure-kusto-ingest==4.2.0" \
    "azure-identity==1.14.0" \
    "azure-core==1.29.4" --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
import datetime
import tempfile
import json
import os
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties
from azure.kusto.ingest.ingestion_properties import DataFormat

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class FabricKQLHandler(logging.Handler):
    def __init__(self, cluster: str, database: str, table: str, pipeline: str, notebook: str):
        super().__init__()
        self.cluster = cluster
        self.database = database
        self.table = table
        self.pipeline = pipeline
        self.notebook = notebook

        # Use interactive login — Fabric manages identity internally
        kcsb = KustoConnectionStringBuilder.with_interactive_login(
            f"https://ingest-{cluster}.kusto.windows.net"
        )
        self.client = QueuedIngestClient(kcsb)

        self.ingestion_props = IngestionProperties(
            database=self.database,
            table=self.table,
            data_format=DataFormat.JSON
        )

    def emit(self, record):
        log_entry = {
            "Timestamp": datetime.datetime.utcnow().isoformat(),
            "Level": record.levelname,
            "Pipeline": self.pipeline,
            "Notebook": self.notebook,
            "Step": record.name,
            "Message": record.getMessage(),
            "Details": {}
        }

        try:
            with tempfile.NamedTemporaryFile('w', suffix=".json", delete=False) as tmp:
                tmp.write(json.dumps(log_entry) + "\n")
                tmp.flush()
                file_path = tmp.name

            self.client.ingest_from_file(file_path, ingestion_properties=self.ingestion_props)

        except Exception as e:
            print(f"[Logging Error] Failed to ingest log: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set up logger for your ETL step
logger = logging.getLogger("transform_step_1")
logger.setLevel(logging.INFO)

# Add handler (run only once per notebook)
kql_handler = FabricKQLHandler(
    cluster="au58pnjgtdtk304kxd",          # e.g., "westeurope-1"
    database="",    # e.g., "MyEventhouseDB"
    table="etl_logs",
    pipeline="bronze_to_gold",
    notebook="bronze_to_gold_transform"
)
logger.addHandler(kql_handler)

# Log messages
logger.info("Starting transformation step")
logger.warning("Some records had missing values")
logger.error("Transformation failed for customer ID 123")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load KQLmagic (already installed in Fabric)
%load_ext Kqlmagic

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Connect to your Eventhouse KQL database
# Replace YOUR_CLUSTER and YOUR_DB with your actual names
%kql azureDataExplorer://code;cluster='https://au58pnjgtdtk304kxd.z6.kusto.windows.net';database='Logging_Eventhouse_DEV'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reusable logger function
from datetime import datetime
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def log_to_kql(level, pipeline, notebook, step, message, details=None):
    timestamp = datetime.utcnow().isoformat() + "Z"
    details_json = json.dumps(details or {})
    
    kql_inline = f"""{timestamp}, "{level}", "{pipeline}", "{notebook}", "{step}", "{message}", {details_json}"""
    
    %kql 
    .ingest inline into table etl_logs <|
    {kql_inline}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log_to_kql(
    level="INFO",
    pipeline="bronze_to_gold",
    notebook="bronze_to_gold_transform",
    step="step1_load_bronze",
    message="Started processing bronze data"
)

log_to_kql(
    level="ERROR",
    pipeline="bronze_to_gold",
    notebook="bronze_to_gold_transform",
    step="step2_clean_data",
    message="Null values found in critical column",
    details={"column": "customer_id", "null_count": 43}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
