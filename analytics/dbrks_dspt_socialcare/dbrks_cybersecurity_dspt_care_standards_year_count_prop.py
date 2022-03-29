# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_cybersecurity_dspt_care_standards_year_count_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: Number and percent of adult social care organisations that meet or exceed the DSPT standard (M011 & M012)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        6 Dec 2021
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
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
file_system = config_JSON['pipeline']['adl_file_system']
sink_path = config_JSON['pipeline']['project']['sink_path']
sink_file = config_JSON['pipeline']['project']['sink_file']  

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------

latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df["Count"] = 1
df_1 = df.groupby(['Date',"CQC registered location - latest DSPT status"]).sum().reset_index()

#Makes changed as outlined in the SOP when the finanical year flag for Standards Met/Exceeded changes
#----------------------------------------------------------------------------------------------------
#19/20 and 20/21 FY
#-----------------------------------
df_2 = df_1.loc[df_1['Date'] <= '2021-09']
df_3 = df_2[
(df_2["CQC registered location - latest DSPT status"] == "19/20 Standards Met.") |
(df_2["CQC registered location - latest DSPT status"] == "19/20 Standards Exceeded.") |
(df_2["CQC registered location - latest DSPT status"] == "20/21 Standards Met.") |
(df_2["CQC registered location - latest DSPT status"] == "20/21 Standards Exceeded.")
].reset_index(drop=True)
df_4 = df_3.groupby("Date").sum().reset_index()

#20/21 and 21/22 FY
#---------------------------------
df_5 = df_1.loc[df_1['Date'] >= '2021-10'] #------------- change filter once new finanical year added.
df_6 = df_5[
(df_5["CQC registered location - latest DSPT status"] == "20/21 Standards Met.") |
(df_5["CQC registered location - latest DSPT status"] == "20/21 Standards Exceeded.") |
(df_5["CQC registered location - latest DSPT status"] == "21/22 Standards Met.") |
(df_5["CQC registered location - latest DSPT status"] == "21/22 Standards Exceeded.")
].reset_index(drop=True)
df_7 = df_6.groupby("Date").sum().reset_index()

#21/22 and 22/23 FY
#---------------------------------
# Add code here

#---------------------------------------------------------------------------------------
df_fy_appended = pd.concat([df_4, df_7]).reset_index(drop = True) #------------- Once a new finanical year is added appened additional FY dataframe. ie pd.concat([df_4, df_7, df_10]).reset_index(drop = True)
#---------------------------------------------------------------------------------------
df_dates = df_1.groupby("Date").sum().reset_index()
df_dates_merged = df_fy_appended.merge(df_dates, on = 'Date', how = 'left')
df_dates_merged_1 = df_dates_merged.rename(columns = { 'Count_x':'Number of social care organizations with a standards met or exceeded DSPT status', 'Count_y':'Total number of social care organizations'})
df_dates_merged_1["Percent of social care organizations with a standards met or exceeded DSPT status"] = df_dates_merged_1['Number of social care organizations with a standards met or exceeded DSPT status']/df_dates_merged_1['Total number of social care organizations']
df_dates_merged_2 = df_dates_merged_1 .round(4)
df_dates_merged_2.index.name = "Unique ID"
df_processed = df_dates_merged_2.copy()

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
