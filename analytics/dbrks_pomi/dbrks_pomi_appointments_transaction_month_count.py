# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_pomi_appointments_transaction_month_count.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: No. of GP appointment management transactions (M053)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        01 Sept 2021
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.*

# COMMAND ----------

# Imports
# -------------------------------------------------------------------------
# Python:
import os
import io
import tempfile
from datetime import datetime
import json

# 3rd party:
import pandas as pd
import numpy as np
from pathlib import Path
from azure.storage.filedatalake import DataLakeServiceClient

# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope="datalakefs", key="CONNECTION_STRING")

# COMMAND ----------

# MAGIC %run /Repos/prod/au-azure-databricks/functions/dbrks_helper_functions

# COMMAND ----------

#Download JSON config from Azure datalake
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_pomi_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
file_system = config_JSON['pipeline']['adl_file_system']
sink_path = config_JSON['pipeline']['project']['databricks'][5]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][5]['sink_file']  

# COMMAND ----------

#Processing
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df = df[df["Field"] == "Pat_Appts_Use"]
df["Report_Period_End"] = df["Report_Period_End"].astype("datetime64[ns]")
df1 = df.sort_values("Report_Period_End")
df2 = df1.reset_index(drop = True)
df2 = df2.drop(columns={
  "Field", 
  "Region_Code", 
  "CCG_Code", 
  "Subregion_Code", 
  "System_Supplier", 
  "Effective_Snapshot_Date", 
  "DataSourceFileForThisSnapshot_Version", 
  "Report_Period_Length", 
  "Unique_ID", 
  "AuditKey"})
df2.rename(columns={
              "Value": "Number of GP appointments managed online",
              "Report_Period_End": "Date",
              "Practice_Code": "Practice code"},
               inplace=True)
df2.index.name = "Unique ID"
df_processed = df2.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
