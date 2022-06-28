# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_national_digital_channels_highlights_raw.py
DESCRIPTION:
                Databricks notebook with code to ingest new raw data for the NHSX Analytics unit metrics within 
                the National Digital Channels (NDC) Dashboard Porject
USAGE:
                ...
CONTRIBUTORS:   Chris Todd
CONTACT:        data@nhsx.nhs.uk
CREATED:        28th July. 2022
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
file_name_config = "config_national_digital_channels_highlights_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = config_JSON['pipeline']['adl_file_system']
new_source_path = config_JSON['pipeline']['raw']['source_path']
appended_path = config_JSON['pipeline']['raw']['appended_path']
appended_file = config_JSON['pipeline']['raw']['appended_file']

# COMMAND ----------

# Pull Highlights dataset
# ----------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)
source_file  = [file for file in file_name_list if '.xlsx' in file][0]

new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, source_file)
new_data = pd.read_excel(io.BytesIO(new_dataset), sheet_name = ['Highlights'], engine='openpyxl')

new_data_df = new_data['Highlights']

monthly_highlights_df = new_data_df.copy()  

# COMMAND ----------

# Upload merged data to datalake
# -------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
monthly_highlights_df.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, appended_path+current_date_path, appended_file)
