# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_gp_patient_survey_results_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical
                data for the NHSX Analyticus unit metrics within the GP patient survey
                topic
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        20 Jan 2021
VERSION:        0.0.2
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install pandas pathlib azure-storage-file-datalake numpy pyarrow==5.0.*

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
file_name_config = "config_gp_patient_survey_dbrks.json"
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
file_name_list = [file for file in file_name_list if '.csv' in file]
for new_source_file in file_name_list:
  new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
  fields = ['Practice_Code', 'Q114base', 'Q114_5', 'Q101base', 'Q101_4', 'q73base', 'Q73_1234base', 'q73_12' ] #----------- Change values year-on-year to select only the columns corresponding to the specific                                                                                                                    questions analysed. Please see SOP. 
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset),usecols=fields)

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
# Latest data processing
new_dataframe.rename(columns = {'Practice_Code': 'Practice code',  #----------- Change values year-on-year to select only the columns corresponding to the specific questions analysed. Please see SOP.
                           'Q114base': 'M090_denominator', 
                           'Q114_5': 'M090_numerator',
                           'Q101base': 'M091_denominator',
                           'Q101_4': 'M091_numerator',
                           'q73base': 'M092_denominator', 
                           'Q73_1234base': 'M092_numerator_M093_denominator', 
                           'q73_12': 'M093_numerator'},
                           inplace=True)
new_dataframe.insert(0,'Date','')
new_dataframe.insert(0,'Collection end date','')
new_dataframe.insert(0,'Collection start date','')
date = str(latestFolder[0:10])
new_dataframe["Date"] = date
new_dataframe["Collection start date"] = "2021-01-01" #----------- Change values year-on-year. Please see SOP.
new_dataframe["Collection end date"] = "2021-03-01" #----------- Change values year-on-year. Please see SOP.

# COMMAND ----------

# Pull historical dataset
latestFolder_historical = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder_historical, historical_source_file)
historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")

# COMMAND ----------

# Append new data to historical data
# -------------------------------------------------------------------------
if date not in historical_dataframe["Date"].values:
  historical_dataframe = historical_dataframe.append(new_dataframe)
  historical_dataframe = historical_dataframe.reset_index(drop=True)
  historical_dataframe.index.name = "Unique ID"
else:
  print("data already exists")

# COMMAND ----------

# Upload processed data to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
historical_dataframe.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
