# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_socialcare_digitalrecord_month_count_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: Proportion of CQC registered social care providers that have adopted a digital social care record (M013)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        21 Apr. 2022
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
file_name_config = "config_digitalrecords_socialcare_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
denom_source_path = config_JSON['pipeline']['project']['reference_source_path']
denom_source_file = config_JSON['pipeline']['project']['reference_source_file']
file_system = config_JSON['pipeline']['adl_file_system']
sink_path = config_JSON['pipeline']['project']['sink_path']
sink_file = config_JSON['pipeline']['project']['sink_file']  

# COMMAND ----------

# Numerator Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_1 = df[["Location ID", "Location Status", "PIR submission date", "Use a Digital Social Care Record system?"]]
df_1['PIR submission date'] = pd.to_datetime(df_1['PIR submission date']).dt.strftime('%Y-%m')
df_2 = df_1[~df_1.duplicated(['Location ID', 'Use a Digital Social Care Record system?'])].reset_index(drop = True)
df_2['Use a Digital Social Care Record system?'] = df_2['Use a Digital Social Care Record system?'].replace('Yes',1).replace('No',0)
df_3 = df_2[df_2['Location Status'] == 'Active']
df_4 = df_3.groupby(['PIR submission date'])['Use a Digital Social Care Record system?'].agg(['sum', 'count']).reset_index()
df_4[['sum', 'count']] = df_4[['sum', 'count']].cumsum()
df_4 = df_4.rename(columns = {'PIR submission date': 'Date', 'sum': 'Cummulative number of adult socialcare providers that have adopted a digital social care record', 'count': 'Cummulative number of adult socialcare providers that returned a PIR'})

# Denom Processing
# -------------------------------------------------------------------------
latestFolder_denom = datalake_latestFolder(CONNECTION_STRING, file_system, denom_source_path)
file = datalake_download(CONNECTION_STRING, file_system, denom_source_path+latestFolder_denom, denom_source_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_ref_1 = df_ref[['Location CQC ID ', 'Dormant (Y/N)','Date']]
df_ref_2= df_ref_1[df_ref_1['Dormant (Y/N)'] == 'N'].reset_index(drop = True)
df_ref_3=df_ref_2.groupby('Date').count().reset_index().drop(columns = 'Dormant (Y/N)')
df_ref_4 = df_ref_3.rename(columns = {'Location CQC ID ': 'Number of active adult socialcare organisations'})

# COMMAND ----------

# Joint processing
# -------------------------------------------------------------------------
df_join = df_4.merge(df_ref_4, how ='left', on = 'Date')
df_join['Percentage of adult socialcare providers that have adopted a digital social care record']= df_join['Cummulative number of adult socialcare providers that have adopted a digital social care record']/df_join['Number of active adult socialcare organisations']
df_join.index.name = "Unique ID"
df_join = df_join.round(4)
df_processed = df_join.copy()

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
