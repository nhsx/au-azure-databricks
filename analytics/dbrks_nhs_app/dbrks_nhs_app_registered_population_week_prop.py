# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_nhs_app_registered_population_week_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric M047: % adult population with an NHS App registration
USAGE:
                ...
CONTRIBUTORS:   Chris Todd, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        23 Feb 2022
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
file_name_config = "config_nhs_app_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = config_JSON['pipeline']['adl_file_system']
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_source_path = config_JSON['pipeline']['project']['reference_source_path']
reference_source_file = config_JSON['pipeline']['project']['reference_source_file']
sink_path = config_JSON['pipeline']['project']['databricks'][0]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][0]['sink_file']  

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

#Numerator
# ---------------------------------------------------------------------------------------------------
df['Date'] = pd.to_datetime(df['Date'], infer_datetime_format=True)  
df.rename(columns = {'AcceptedTermsAndConditions':'users'}, inplace = True)
df2 = df[['Date','users']].copy()
df2['users'] = pd.to_numeric(df2['users'],errors='coerce').fillna(0)
df2.drop(df2[df2['Date'] < '2021-01-01'].index, inplace = True) #--------- remove rows pre 2021
df2 = df2.groupby('Date').sum().resample('W').sum()
df2['total_users'] = df2['users'].cumsum() #--------- add cumulative sum column

#Denominator porcessing
# ---------------------------------------------------------------------------------------------------
df_ref.loc[df_ref['Age'] == "90+", 'Age'] = 90
df_ref['Age'] = df_ref['Age'].astype('int32')
df_ref_latest_adult = df_ref[(df_ref['Effective_Snapshot_Date'] == df_ref['Effective_Snapshot_Date'].max()) & ((df_ref['Age'] >17))]
denominator = df_ref_latest_adult['Size'].sum()

#Joint processing 
# ---------------------------------------------------------------------------------------------------
df3 = df2.reset_index()
df3['Adult population'] = denominator
df3['Percentage of adult population with an NHS App registration'] = df3['total_users']/denominator
df4 = df3.drop(['users'], axis=1).rename(columns = {'total_users': 'Number of users with an NHS App registration'})
df5 = df4.round(4)
df5.index.name = "Unique ID"
df_processed = df5.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
