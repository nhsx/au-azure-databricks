# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           ddbrks_nhs_app_uptake_gp_registered_population_day_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric M0148: % of GP Patients Registered for NHS App (GP practice level)
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        17th May 2022
VERSION:        0.0.2
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
file_name_config = "config_nhs_app_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = config_JSON['pipeline']['adl_file_system']
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_source_path = config_JSON['pipeline']['project']['reference_source_path_gp']
reference_source_file = config_JSON['pipeline']['project']['reference_source_file_gp']
sink_path = config_JSON['pipeline']['project']['databricks'][5]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][5]['sink_file']

# COMMAND ----------

# Ingestion of numerator data (NHS app performance data)
# ---------------------------------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# Ingestion of reference deomintator data (ONS: age banded population data)
# ---------------------------------------------------------------------------------------------------
ref_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_source_path)
file = datalake_download(CONNECTION_STRING, file_system, reference_source_path+ref_latestFolder, reference_source_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# COMMAND ----------

#Processing
# ---------------------------------------------------------------------------------------------------
df1 = df[["Date", "OdsCode", "P9VerifiedNHSAppUsers"]].copy()
df1['Date'] = pd.to_datetime(df1['Date'], infer_datetime_format=True)
df2 = df1[df1['Date'] >= '2021-01-01'].reset_index(drop = True)  #--------- remove rows pre 2021
df2["P9VerifiedNHSAppUsers"] = pd.to_numeric(df2["P9VerifiedNHSAppUsers"],errors='coerce').fillna(0)
df2=df2.sort_values(['Date']).reset_index(drop=True)
df2["Cumulative number of P9 NHS app registrations"]=df2.groupby(['OdsCode'])["P9VerifiedNHSAppUsers"].cumsum(axis=0)
df3 = df2.drop(columns = ["P9VerifiedNHSAppUsers"]).reset_index(drop = True)
df4 = df3.rename(columns = {'OdsCode': 'Practice code'})

# COMMAND ----------

#Denominator porcessing
# ---------------------------------------------------------------------------------------------------
#Get all dates from the NHS app data
# ---------------------------------------------------------------------------------------------------
df_date = pd.DataFrame({'Date': df4['Date'].unique()})
df_date['join_code'] = 'X3003'
# Add dates to most recent GP population snapshot
# ----------------------------------------------------------------------------------------------------
df_ref_1 = df_ref.rename(columns = {'GP_Practice_Code': 'Practice code', 'Registered_patient': 'Number of GP registered patients', 'Effective_Snapshot_Date': 'Snapshot date for GP Population data'})
df_ref_1['join_code'] = 'X3003'
df_ref_2 = df_date.merge(df_ref_1, how = 'outer', on = 'join_code').drop(columns = ['join_code'])

#Joint data processing
# ---------------------------------------------------------------------------------------------------
df_join = df4.merge(df_ref_2, how = 'outer', on = ['Date', 'Practice code'])
df_join['Cumulative number of P9 NHS app registrations'] = df_join['Cumulative number of P9 NHS app registrations'].fillna(0)
df_join['Number of GP registered patients'] = df_join['Number of GP registered patients'].fillna(0)
df_join['Snapshot date for GP Population data'] = df_join['Snapshot date for GP Population data'].fillna(df_ref_1['Snapshot date for GP Population data'].max())
df_join.index.name = "Unique ID"
df_processed = df_join.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
