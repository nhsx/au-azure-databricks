# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_primarycare_gp_records_api_month_count.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: Number of GP records accessed using the digital API* by other NHS organisations (* Direct Care API) (M016)
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
file_name_config = "config_gp_records_api_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
file_system = config_JSON['pipeline']['adl_file_system']
sink_path = config_JSON['pipeline']['project']['sink_path']
sink_file = config_JSON['pipeline']['project']['sink_file']  

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_csv(io.BytesIO(file))
df1 = df.drop([
        'Number of GP Practices in England enabled to share records', 
        'Percent of GP Practices in England enabled to share records',
        'Number of appointments booked for patients',
        'Number of GP practices in England engaged in an appointment transaction',
        'Percent of GP practices in England engaged in an appointment transaction',
        'Number of GP practices in England that have been enabled to share appointments',
        'Percent of GP practices in England that have been enabled to share appointments',
        'Unique ID'
        ], 
        axis=1
    )
df1['Date of extract'] = pd.to_datetime(df1['Date of extract'])
df1 = df1.rename(columns = {'Date of extract': 'Date'})
df2 = df1.copy()
df3 = df2.set_index('Date')
resampled_date_list = []
resampled_date_df = pd.DataFrame()
resampled_date_list = df3.groupby(df3.index.month).apply(lambda x: x.index.max())
resampled_date_df['Date'] = resampled_date_list 
resampled_date_df = resampled_date_df.reset_index(drop = True)
resampled_date_df_1 = resampled_date_df.merge(df1, how='left', on='Date')
resampled_date_df_1 = resampled_date_df_1.sort_values(by='Date', ascending=True).reset_index(drop = True)
df4 = resampled_date_df_1.rename(columns = {'Date of extract': 'Date', 'Number of patient records viewed': 'Number of GP records accessed using the digital API'})
df4['Date'] = df4['Date'].dt.strftime("%Y-%m")
df4.index.name = "Unique ID"
df_processed = df4.copy()

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
