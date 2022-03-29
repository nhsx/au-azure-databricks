# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_dspt_socialcare_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical
                data for the NHSX Analytics unit metrics within the topic DSPT socialcare
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        06 Dec. 2021
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* xlrd openpyxl python-dateutil

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
import requests
from pathlib import Path
from urllib import request as urlreq
from bs4 import BeautifulSoup
from azure.storage.filedatalake import DataLakeServiceClient
from dateutil.relativedelta import relativedelta

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
file_name_config = "config_dspt_socialcare_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = config_JSON['pipeline']['adl_file_system']
new_source_path = config_JSON['pipeline']['raw']['snapshot_source_path']
historical_source_path = config_JSON['pipeline']['raw']['appended_path']
historical_source_file = config_JSON['pipeline']['raw']['appended_file']
sink_path = config_JSON['pipeline']['raw']['appended_path']
sink_file = config_JSON['pipeline']['raw']['appended_file']

# COMMAND ----------

# Pull new snapshot dataset
# -------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)
file_name_list = [file for file in file_name_list if '.xlsx' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  header_list = ["Unnamed","CQC registered location - latest DSPT status", "Date of location publication", "Location CQC ID ", "Location start date", "Care home?", "Location name", "Location ODS code", "Location telephone number", "CQC registered manager","Location region","Region","Location local authority","Location ONSPD CCG","Location street address","Location address line 2", "Location city", "Location county", "Location postal code", "Brand ID", "Brand name", "Name of parent organisation", "CQC ID of parent organisation", "Larger organisation?", "Single Location", "Parent ODS code", "Latest DSPT status of parent", "Dormant (Y/N)"]
  new_dataframe = pd.read_excel(io.BytesIO(new_dataset), sheet_name = 'Line By Line', header = 4, engine='openpyxl', names = header_list)
  new_dataframe_1 = new_dataframe.loc[:, ~new_dataframe.columns.str.contains('^Unnamed')]
  new_dataframe_1['Date'] = latestFolder.replace('/','')
  new_dataframe_1['Date'] = pd.to_datetime(new_dataframe_1['Date']).dt.strftime('%Y-%m')

# COMMAND ----------

# Pull historical dataset
# -----------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder, historical_source_file)
historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")
historical_dataframe['Date'] = pd.to_datetime(historical_dataframe['Date']).dt.strftime('%Y-%m')

# COMMAND ----------

# Append new data to historical data
# -----------------------------------------------------------------------
date_from_new_dataframe = new_dataframe_1['Date'].values.max()
if date_from_new_dataframe != historical_dataframe['Date'].values.max():
  historical_dataframe = historical_dataframe.append(new_dataframe_1)
  historical_dataframe = historical_dataframe.sort_values(by=['Date'])
  historical_dataframe = historical_dataframe.reset_index(drop=True)
  historical_dataframe = historical_dataframe.astype(str)
else:
  print("data already exists")

# COMMAND ----------

# Upload hsitorical appended data to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
historical_dataframe.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
