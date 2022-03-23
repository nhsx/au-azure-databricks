# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_nhs_app_logins_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical
                data for the NHSX Analytics unit metrics within the topic nhs app logins count
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli, Everistus Oputa, Chris Todd
CONTACT:        data@nhsx.nhs.uk
CREATED:        15 Mar. 2022
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
import requests
from pathlib import Path
from urllib import request as urlreq
from bs4 import BeautifulSoup
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
file_name_config = "config_nhs_app_logins_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------



# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = config_JSON['pipeline']['adl_file_system']
new_source_path = config_JSON['pipeline']['raw']['snapshot_source_path'] #"land/datainbox/email_attachment/alerts@splunkcloud.com/Splunk Alert: Last 6 Months Unique Accounts and Total logins by Week/"
historical_source_path = config_JSON['pipeline']['raw']['appended_path']
historical_source_file = config_JSON['pipeline']['raw']['appended_file']
sink_path = config_JSON['pipeline']['raw']['appended_path']
sink_file = config_JSON['pipeline']['raw']['appended_file']


# COMMAND ----------

# Pull new snapshot dataset
# -------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
nhs_login_file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)
for new_source_file in nhs_login_file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset))
  
new_dataframe.rename({"_time": "date"}, axis = 'columns', inplace = True)
new_dataframe['date'] = pd.to_datetime(new_dataframe['date'], format='%c')

# COMMAND ----------

# Pull historical dataset
# -----------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder, historical_source_file)
historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")
historical_copy = historical_dataframe.copy()

# COMMAND ----------

# Combine new data with historic data
# -----------------------------------------------------------------------
#Used sets instead of lists as they will run faster. 
existing_dates = set(historical_dataframe['date'])
new_dates = set(new_dataframe['date'])

#loop through each row of data in new file. If the data already exists in the historic data, overwrite it with new data. If it doesn't, append it to end.  
for date in new_dates:
 if date in existing_dates:
  historical_dataframe[historical_dataframe['date'] == date] = new_dataframe[new_dataframe['date'] == date]
 else:
  historical_dataframe = historical_dataframe.append(new_dataframe[new_dataframe['date'] == date])

historical_dataframe = historical_dataframe.sort_values(by=['date'])
historical_dataframe = historical_dataframe.reset_index(drop=True)

# COMMAND ----------

# Upload hsitorical appended data to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
historical_dataframe.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)

# COMMAND ----------

historical_dataframe
