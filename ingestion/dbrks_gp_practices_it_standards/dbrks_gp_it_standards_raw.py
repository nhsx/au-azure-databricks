# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_gp_it_standards_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical
                data for the NHSX Analyticus unit metrics within the GP IT standards topic
                topic
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        01 Feb. 2021
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# ------------------------------------------------------------------------------------
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
file_name_config = "config_gp_it_standards_dbrks.json"
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
  new_dataframe = pd.read_csv(io.BytesIO(new_dataset)) #------- Check if file is in .csv format. Please see SOP.
  new_dataframe["Financial Year"] = "2020/2021" #----------- Change values year-on-year to the correct financial year. Please see SOP.
  new_dataframe["Date"] = new_dataframe["Financial Year"].unique().tolist()[0][5:9]+"-01-01"

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
# Latest data processing
new_dataframe = new_dataframe.rename(columns = {"Practice ODS code": "Practice ODS Code"}) #------ Rename columns so that they align to the historical dataset. Please see SOP.
df_processed = new_dataframe[new_dataframe["Practice ODS Code"].str.contains("Grand Total")==False] #------ Check a total has not been added to the file. Please see SOP.

# COMMAND ----------

# Pull historical dataset
latestFolder_historical = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder_historical, historical_source_file)
historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")

# Append new data to historical data
# -----------------------------------------------------------------------
years_in_historical = historical_dataframe["Date"].unique().tolist()
years_in_new = df_processed["Date"].unique().tolist()[0]
if years_in_new in years_in_historical:
  print('Data already exists in historical data')
else:
  historical_dataframe = historical_dataframe.append(df_processed)
  historical_dataframe = historical_dataframe.sort_values(by=['Date'])
  historical_dataframe = historical_dataframe.reset_index(drop=True)
  historical_dataframe = historical_dataframe.astype(str)

# COMMAND ----------

# Upload processed data to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
historical_dataframe.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
