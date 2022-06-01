# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_ndc_channel_shift_adult_population_offline_mount_count.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric M247: Adult population offline
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        1st June 2022
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
file_name_config = "config_national_digital_channels_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = config_JSON['pipeline']['adl_file_system']
source_path = config_JSON['pipeline']['project']["reference_source_path"]
source_file = config_JSON['pipeline']['project']["reference_source_file"]
reference_source_path = config_JSON['pipeline']['project']["reference_source_path_pomi"]
reference_source_file = config_JSON['pipeline']['project']["reference_source_file_pomi"]
sink_path = config_JSON['pipeline']['project']['databricks'][4]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][4]['sink_file']  

# COMMAND ----------

# Ingestion of reference deomintator data (ONS population)
# ---------------------------------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# Ingestion of reference deomintator data (POMI)
# ---------------------------------------------------------------------------------------------------
ref_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_source_path)
file = datalake_download(CONNECTION_STRING, file_system, reference_source_path+ref_latestFolder, reference_source_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# COMMAND ----------

#Processing
# ---------------------------------------------------------------------------------------------------

#ONS population
# ---------------------------------------------------------------------------------------------------
df.loc[df['Age'] == "90+", 'Age'] = 90
df['Age'] = df['Age'].astype('int32')
df_ref_latest_adult = df[(df['Effective_Snapshot_Date'] == df['Effective_Snapshot_Date'].max()) & ((df['Age'] >17))]
current_population = df_ref_latest_adult['Size'].sum()

#Denominator porcessing (POMI)
# ---------------------------------------------------------------------------------------------------
df_ref_1 = df_ref[["Report_Period_End", "Field", "Value"]]
df_ref_2 = df_ref_1[df_ref_1['Field']=='Total_Pat_Enbld'].reset_index(drop = True)
df_ref_2["Report_Period_End"] = pd.to_datetime(df_ref_2["Report_Period_End"]).dt.strftime('%Y-%m')
df_ref_3 = df_ref_2.groupby('Report_Period_End')['Value'].sum().reset_index()

#Joint processing 
# ---------------------------------------------------------------------------------------------------
df_joint = df_ref_3[df_ref_3['Report_Period_End'] > '2020-12'].reset_index(drop = True)
df_joint.rename(columns = {'Report_Period_End': 'Date', 'Value': 'Total number of patients registered to use an patient online transactional services'}, inplace=True)
df_joint['Adult population'] = current_population
df_joint['Adult population not registered to use an patient online transactional services'] = df_joint['Adult population'] - df_joint['Total number of patients registered to use an patient online transactional services']
df_joint.index.name = "Unique ID"
df_processed = df_joint.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
