# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_gp_eps_results_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical
                data for the NHSX Analyticus unit metrics within the Electronic Prescription Service (EPS)
                topic
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        04 Oct. 2021
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
from dateutil.relativedelta import relativedelta
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
file_name_config = "config_gp_eps_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = config_JSON['pipeline']['adl_file_system']
new_source_path = config_JSON['pipeline']['raw']['sink_path']
new_source_file = config_JSON['pipeline']['raw']['sink_file']
historical_source_path = config_JSON['pipeline']['raw']['appended_path']
historical_source_file = config_JSON['pipeline']['raw']['appended_file']
sink_path = config_JSON['pipeline']['raw']['appended_path']
sink_file = config_JSON['pipeline']['raw']['appended_file']

# COMMAND ----------

#Ingestion of raw .csv from NHSD webpage for metrics:

url = "https://digital.nhs.uk/data-and-information/data-tools-and-services/tools-for-accessing-data/deployment-and-utilisation-hub/electronic-prescription-service-deployment-and-utilisation-data" 
response = urlreq.urlopen(url)
soup = BeautifulSoup(response.read(), "lxml")
data = soup.select_one("a[href*='erd-data']")
csv_url = 'https://digital.nhs.uk' +  data['href']
eps_df_snapshot = pd.read_csv(csv_url)
eps_df_snapshot = eps_df_snapshot.rename(columns = {'% of patients with a nomination': '% with nominated pharm'})

#Extract date from csv URL
date_from_csv = csv_url.partition("erd-data-")[2].partition(".csv")[0].title()
date_from_csv_final = datetime.strptime(date_from_csv, '%B-%Y').strftime('%Y-%m-%d')
if date_from_csv_final > datetime.today().strftime("%Y-%m-%d"):
  date_from_csv_final_fix = (datetime.strptime(date_from_csv, '%B-%Y') - relativedelta(years=1)).strftime("%Y-%m-%d")
  eps_df_snapshot['Date'] = date_from_csv_final_fix #------ Add column date to table
else:
   eps_df_snapshot['Date'] = date_from_csv_final #------ Add column date to table

# COMMAND ----------

# Upload raw data snapshot to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
eps_df_snapshot.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, new_source_path+current_date_path, new_source_file)

# COMMAND ----------

#Pull historical dataset
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder, historical_source_file)
historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")

# Append new data to historical data
# -----------------------------------------------------------------------
if eps_df_snapshot['Date'].max() not in historical_dataframe.values:
  historical_dataframe = historical_dataframe.append(eps_df_snapshot)
  historical_dataframe = historical_dataframe.reset_index(drop=True)
  historical_dataframe.index.name = "Unique ID"
else:
  print("data already exists")


# COMMAND ----------

# Upload processed data to datalake
file_contents = io.BytesIO()
historical_dataframe.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
