# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_cybersecurity_dspt_gp_practices_standards_meet_exceed_year_count_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric:M76A No. and % of GP practices that are compliant with (meet or exceed) the DSPT standard (yearly
                historical) 
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli, Muhammad-Faaiz Shanawas 
CONTACT:        data@nhsx.nhs.uk
CREATED:        20 Jan. 2021
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

# MAGIC %run /Repos/dev/au-azure-databricks/functions/dbrks_helper_functions

# COMMAND ----------

#Download JSON config from Azure datalake
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_dspt_gp_practices_historical_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = config_JSON['pipeline']['adl_file_system']
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
sink_path = config_JSON['pipeline']['project']['databricks'][0]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][0]['sink_file']

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df["Snapshot_Date"] = pd.to_datetime(df["Snapshot_Date"])
df["Edition flag_1"] = df["DSPT_Edition"].str[2:4] + "/" + df["DSPT_Edition"].str[7:] + " STANDARDS EXCEEDED"
df["Edition flag_2"] = df["DSPT_Edition"].str[2:4] + "/" + df["DSPT_Edition"].str[7:] + " STANDARDS MET"
df["Status_Raw"] = df["Status_Raw"].str.upper()
df["Status_Raw"] = df["Status_Raw"].replace({'STANDARDS MET (19-20)':'STANDARDS MET','NONE': 'NOT PUBLISHED'})
df.loc[(df["DSPT_Edition"]=='2018/2019') & (df["Status_Raw"]== 'STANDARDS MET'), "Status_Raw"] = '18/19 STANDARDS MET'
df.loc[(df["DSPT_Edition"]=='2018/2019') & (df["Status_Raw"]== 'STANDARDS EXCEEDED'), "Status_Raw"] = '18/19 STANDARDS EXCEEDED'
def exceed_dspt(c):
  if c['Status_Raw'] == c['Edition flag_1']:
    return 1
  else:
    return 0
def met_dspt(c):
  if c['Status_Raw'] == c['Edition flag_2']:
    return 1
  else:
    return 0
df['Number of GP practices that exceed the DSPT standard (historical)'] = df.apply(exceed_dspt, axis=1)
df['Number of GP practices that met the DSPT standard (historical)'] = df.apply(met_dspt, axis=1)
df["Number of GP practices that meet or exceed the DSPT standard (historical)"] = df['Number of GP practices that exceed the DSPT standard (historical)'] + df['Number of GP practices that met the DSPT standard (historical)']
df.rename(columns={"Code":"Practice code", "DSPT_Edition":"Financial year", "Snapshot_Date": "Date"}, inplace = True)
df1 = df.drop(["Organisation_Name", "Status_Raw", "Edition flag_1", "Edition flag_2", "Number of GP practices that exceed the DSPT standard (historical)", "Number of GP practices that met the DSPT standard (historical)"], axis = 1)
df1.index.name = "Unique ID"
df_processed = df1.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
