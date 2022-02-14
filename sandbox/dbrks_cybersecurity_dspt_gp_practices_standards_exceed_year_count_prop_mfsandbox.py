# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_cybersecurity_dspt_gp_practices_standards_exceed_year_count_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: M077A: No. and % of GP practices that exceed the DSPT standard (yearly historical)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli, Muhammad-Faaiz Shanawas
CONTACT:        data@nhsx.nhs.uk
CREATED:        20 Jan. 2021
VERSION:        0.0.1
CLONE:          Cloned by Martina Fonseca 10 February 2022
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

sink_path = config_JSON['pipeline']['project']['databricks'][1]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][1]['sink_file']

# COMMAND ----------

# Interim - parameters for denominator file
# file_system # assume same as above
den_source_path = 'test/sink_data/martina_fonseca/dspt_gp_denom/'
den_source_file = 'processed.csv'


# COMMAND ----------

# Loading - denominator
den_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, den_source_path) # in practice no subf for now
den_file = datalake_download(CONNECTION_STRING, file_system, den_source_path+den_latestFolder, den_source_file)
df_den = pd.read_csv(io.BytesIO(den_file))
df_den

# COMMAND ----------

# Processing - bringing in DSPT
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df["Snapshot_Date"] = pd.to_datetime(df["Snapshot_Date"])
df_num=df
df_num

# COMMAND ----------

# Processing - merge denominator ("ground-truth for practices") and numerator ("DSPT status"). Left join (anything not found in denominator dropped.)

df = df_den.merge(df_num,'left',left_on=['PRACTICE_CODE','FY'],right_on=['Code','DSPT_Edition'])

# COMMAND ----------

# Processing - continued, with merged file
# -------------------------------------------------------------------------
df["Edition flag"] = df["DSPT_Edition"].str[2:4] + "/" + df["DSPT_Edition"].str[7:] + " STANDARDS EXCEEDED"
df["Status_Raw"] = df["Status_Raw"].str.upper()
df["Status_Raw"] = df["Status_Raw"].replace({'STANDARDS MET (19-20)':'STANDARDS MET','NONE': 'NOT PUBLISHED'})
df.loc[(df["DSPT_Edition"]=='2018/2019') & (df["Status_Raw"]== 'STANDARDS MET'), "Status_Raw"] = '18/19 STANDARDS MET'
df.loc[(df["DSPT_Edition"]=='2018/2019') & (df["Status_Raw"]== 'STANDARDS EXCEEDED'), "Status_Raw"] = '18/19 STANDARDS EXCEEDED'
def exceed_dspt(c):
  if c['Status_Raw'] == c['Edition flag']:
    return 1
  else: # MFo : I think this continues being robust to the NaN case. i.e. if not matched, assign 0. Might need to change for M079 (not published), as NaN would be a '1'. Based on df_processed below, looks fine
    return 0
df['Number of GP practices that exceed the DSPT standard (historical)'] = df.apply(exceed_dspt, axis=1)
df.rename(columns={"PRACTICE_CODE":"Practice code", "FY":"Financial year", "Snapshot_Date": "Date"}, inplace = True)
df1 = df.drop(["Organisation_Name", "Status_Raw", "Edition flag","DSPT_Edition","Code","PRACTICE_NAME","Date","EXTRACT_DATE"], axis = 1) # mFo: remove other duplicates
df1.index.name = "Unique ID"
df_processed = df1.copy()

# COMMAND ----------

df_processed

# COMMAND ----------

## QA / sense-check - to be commented out
df_processed.groupby('Financial year').describe()

# QA'd the 2019/2020 value for the mean, i.e. % exceeding, which is 1.564% . Fairly internally consistent with https://docs.google.com/presentation/d/1kaTNFqx_iYcfPPwQ-q4Lzz8UjUeBDK22/edit?usp=sharing&ouid=110109789921646059149&rtpof=true&sd=true, 1.57%

# COMMAND ----------

## QA / sense-check - to be commented out . #107 is the right number for 2019/2020 (at least with analogous pipeline to deep dive analysis)
df_processed.groupby(['Financial year','Number of GP practices that exceed the DSPT standard (historical)']).describe()

# COMMAND ----------

## MFo: commented out to not mess up with currently uploaded files
#Upload processed data to datalake
#file_contents = io.StringIO()
#df_processed.to_csv(file_contents)
#datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
