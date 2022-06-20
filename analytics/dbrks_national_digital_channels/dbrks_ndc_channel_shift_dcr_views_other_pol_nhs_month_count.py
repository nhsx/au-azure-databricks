# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_ndc_channel_shift_dcr_views_other_pol_nhs_month_count.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analytics unit metric M252: DCR views through other POL service
USAGE:
                ...
CONTRIBUTORS:   Chris Todd
CONTACT:        data@nhsx.nhs.uk
CREATED:        16th June 2022
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
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']["source_file_daily"]
reference_source_path = config_JSON['pipeline']['project']["reference_source_path_pomi"]
reference_source_file = config_JSON['pipeline']['project']["reference_source_file_pomi"]
sink_path = config_JSON['pipeline']['project']['databricks'][39]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][39]['sink_file']  

# COMMAND ----------

# Ingestion of numerator data (NHS app performance data)
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

#Numerator
# ---------------------------------------------------------------------------------------------------
df1 = df[["Daily", "RecordViewsDCR"]].copy()
df1.rename(columns  = {'Daily': 'Date', "RecordViewsDCR": 'Number of DCR views through the NHS app'}, inplace = True)
df1 = df1.resample('M', on='Date').sum().reset_index()
df1['Date'] = df1['Date'].dt.strftime('%Y-%m')
df1.index.name = "Unique ID"

# # #Denominator porcessing
# # # ---------------------------------------------------------------------------------------------------
df_ref1 = df_ref.loc[df_ref['Field']=='Pat_DetCodeRec_Use', ["Report_Period_End", "Field", "Value"]].copy()
df_ref1.rename(columns = {'Value': 'DCR views through other POL service'}, inplace=True)
df_ref1["Report_Period_End"] = pd.to_datetime(df_ref1["Report_Period_End"])
df_ref1 = df_ref1.resample('M', on='Report_Period_End').sum().reset_index()
df_ref1['Report_Period_End'] = df_ref1['Report_Period_End'].dt.strftime('%Y-%m')

# # #Joint processing 
# # # ---------------------------------------------------------------------------------------------------

df_joint = df1.merge(df_ref1, how = 'inner', left_on = 'Date', right_on = 'Report_Period_End')
df_joint = df_joint.drop(columns = ['Report_Period_End'])
df_joint['DCR views through other POL service'] = df_joint['DCR views through other POL service'] - df_joint['Number of DCR views through the NHS app']
df_joint.index.name = "Unique ID"
df_processed = df_joint.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
