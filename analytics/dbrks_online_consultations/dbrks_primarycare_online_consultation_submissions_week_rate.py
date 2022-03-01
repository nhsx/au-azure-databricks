# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_primarycare_online_consultation_submissions_week_rate.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: Rate of patient online consultation submissions per week (per 1,000) (M042)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        23 Nov 2021
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

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_online_consult_dbrks.json"
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
df = pd.read_csv(io.BytesIO(file))
df.rename(columns={"oc_submissions_total": "Number of patient online consultation submissions","Practice_Population": "Practice population"},inplace=True,)
df1 = df.reset_index(drop=True)
df2 = df1.groupby(["Week Commencing"])
df3 = df2["Number of patient online consultation submissions"].aggregate(np.sum) / (df2["Practice population"].aggregate(np.sum)/1000) 
df3 = df3.reset_index()
df3 = df3.sort_values("Week Commencing")
df3.rename(columns={0: "Rate of patient online consultation submissions per week (per 1000 practice population)"},inplace=True,)
df3["3 month rolling average"] = (df3["Rate of patient online consultation submissions per week (per 1000 practice population)"].rolling(window=12).mean())
df3 = df3.round(2)
s1 = df2["Number of patient online consultation submissions"].aggregate(np.sum)
s2 = df2["Practice population"].aggregate(np.sum)
df4 = s1.to_frame()
df5 = s2.to_frame()
df6 = df4.merge(df5, left_on='Week Commencing', right_on='Week Commencing')
df7 = df3.merge(df6, left_on='Week Commencing', right_on='Week Commencing')
df7 = df7[[
    'Week Commencing',
    "Number of patient online consultation submissions",
    "Practice population", 
    "Rate of patient online consultation submissions per week (per 1000 practice population)",
    "3 month rolling average"]]
df7.index.name = "Unique ID"
df_processed = df7.copy()

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
