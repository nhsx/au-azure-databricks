# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_gp_records_api_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical
                data for the NHSX Analyticus unit metrics within the topic GP Records
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        18 Oct. 2021
VERSION:        0.0.2
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml dateparser regex pyarrow==5.0.*

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
import urllib.request
from pathlib import Path
from urllib import request as urlreq
from bs4 import BeautifulSoup
from dateparser.search import search_dates
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
file_system = config_JSON['pipeline']['adl_file_system']
historical_source_path = config_JSON['pipeline']['raw']['source_path']
historical_source_file = config_JSON['pipeline']['raw']['source_file']
sink_path = config_JSON['pipeline']['raw']['appended_path']
sink_file = config_JSON['pipeline']['raw']['appended_file']

# COMMAND ----------

# Scrape new data from NHS Digital webpage
# -----------------------------------------------------------------------
url = "https://digital.nhs.uk/services/gp-connect/deployment-and-utilisation"
response = urllib.request.urlopen(url)
soup = BeautifulSoup(response.read(), "lxml")
date_data = soup.find(lambda tag:tag.name=="p" and "current as of" in tag.text)
latestDate = search_dates(date_data.text)
date = latestDate[0][1].strftime("%Y-%m-%d")
results = []
for pp in soup.select("div.nhsd-m-infographic__headline-box"):
  ptext = (
    pp.text.replace(",", "")
      .replace("%", "")
      .replace("(", " ")
      .replace(")", " ")
       )
  res = [int(i) for i in ptext.split() if i.isdigit()]
  results = results + res
del results[-4:]
results.append(date)
df_new = pd.DataFrame([results])
df_new.columns = [
            "Number of patient records viewed",
            "Number of GP Practices in England enabled to share records",
            "Percent of GP Practices in England enabled to share records",
            "Number of appointments booked for patients",
            "Number of GP practices in England engaged in an appointment transaction",
            "Percent of GP practices in England engaged in an appointment transaction",
            "Number of GP practices in England that have been enabled to share appointments",
            "Percent of GP practices in England that have been enabled to share appointments",
            "Date of extract",
        ]

# COMMAND ----------

# Pull historical dataset
# -----------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder, historical_source_file)
historical_dataframe = pd.read_csv(io.BytesIO(historical_dataset), index_col = 0)
historical_dataframe['Date of extract'] = pd.to_datetime(historical_dataframe['Date of extract']).dt.strftime('%Y-%m-%d')

# Append new data to historical data
# -----------------------------------------------------------------------
if date not in historical_dataframe["Date of extract"].values:
  historical_dataframe = historical_dataframe.append(df_new)
  historical_dataframe = historical_dataframe.reset_index(drop=True)
  historical_dataframe.index.name = "Unique ID"
else:
  print('data already exists')

# COMMAND ----------

# Upload hsitorical appended data to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
historical_dataframe.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
