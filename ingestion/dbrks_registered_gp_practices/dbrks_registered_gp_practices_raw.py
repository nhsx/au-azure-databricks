# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_registered_gp_practices_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical
                data for the Number of registered GP practices (NHS Digital) at the end of a financial year (for use as a denominator in metrics on the DSPT status of GP practices)
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli, Martina Fonseca
CONTACT:        data@nhsx.nhs.uk
CREATED:        14 Feb. 2022
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
file_path_config = "/config/pipelines/reference_tables/"
file_name_config = "config_registered_gp_practices.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = config_JSON['pipeline']['adl_file_system']
new_source_path =  config_JSON['pipeline']['raw']['snapshot_source_path']
historical_source_path = config_JSON['pipeline']['raw']['appended_path']
historical_source_file = config_JSON['pipeline']['raw']['appended_file']
sink_path = config_JSON['pipeline']['raw']['appended_path']
sink_file = config_JSON['pipeline']['raw']['appended_file']

# COMMAND ----------

# Pull new snapshot dataset
# -------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
gp_registered_file_name_list = datalake_listContents(CONNECTION_STRING, file_system, new_source_path+latestFolder)
for new_source_file in gp_registered_file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset))

# COMMAND ----------

# Data Processing for new snapshot 
# --------------------------------

#Function to generate Finanical year from a datetime column
#----------------------------------------------------------
def fy(entry):
  if entry['EXTRACT_DATE'].month > 3:
    return str(entry['EXTRACT_DATE'].year)+"/"+str(entry['EXTRACT_DATE'].year+1)
  else:
    return str(entry['EXTRACT_DATE'].year-1)+"/"+str(entry['EXTRACT_DATE'].year)

#Data processing
#----------------------------------------------------------
new_dataframe['EXTRACT_DATE'] = pd.to_datetime(new_dataframe['EXTRACT_DATE']) 
new_dataframe['FY'] = new_dataframe.apply(fy,axis=1) #----- Apply FY function to dataframe
col_keep = ['PRACTICE_CODE','PRACTICE_NAME','EXTRACT_DATE','FY']
new_dataframe = new_dataframe[col_keep]
new_dataframe = new_dataframe.reset_index(drop = True)

# COMMAND ----------

# Pull historical dataset
# -----------------------------------------------------------------------
latestFolder_historical = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder_historical, historical_source_file)
historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")

# Append new data to historical data
# -----------------------------------------------------------------------
fy_editions_in_historical = historical_dataframe["FY"].unique().tolist()
fy_edition_in_new = new_dataframe["FY"].unique().tolist()[0]
if fy_edition_in_new in fy_editions_in_historical:
  print('New data already exists in historical data')
else:
  historical_dataframe = historical_dataframe.append(new_dataframe)
  historical_dataframe = historical_dataframe.sort_values(by=['FY'])
  historical_dataframe = historical_dataframe.reset_index(drop=True)

# COMMAND ----------

# Upload hsitorical appended data to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
historical_dataframe.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
