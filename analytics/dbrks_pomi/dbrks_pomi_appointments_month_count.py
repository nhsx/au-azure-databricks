# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_pomi_appointments_month_count.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: Total number of GP appointments managed online (M002)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        01 Sept 2021
VERSION:        0.0.1
"""

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

# MAGIC %run /Repos/dev/au-azure-databricks/functions/dbrks_helper_functions

# COMMAND ----------

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_pomi_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
file_system = config_JSON['pipeline']['adl_file_system']
sink_path = config_JSON['pipeline']['project']['databricks'][1]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][1]['sink_file']  

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df = df[df["Field"] == "Pat_Appts_Use"]
df1 = df.groupby(["Report_Period_End", "Field"])
df2 = df1["Value"].aggregate(np.sum)
df2 = df2.reset_index()
df2["Report_Period_End"] = df2["Report_Period_End"].astype("datetime64[ns]")
df2 = df2.sort_values("Report_Period_End")
df2 = df2.reset_index(drop=True)
df2["pandas_SMA_3"] = df2["Value"].rolling(window=3).mean()
df2 = df2.drop(columns="Field")
df2.rename(columns={
  "Value": "Number of GP appointments managed online",
  "pandas_SMA_3": "3 month rolling average",
  "Report_Period_End": "Date"},
           inplace=True,)
df2['3 month rolling average'] = np.floor(pd.to_numeric(df2['3 month rolling average'], errors='coerce')).astype('Int64')
df2.index.name = "Unique ID"
df_processed = df2.copy()

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
