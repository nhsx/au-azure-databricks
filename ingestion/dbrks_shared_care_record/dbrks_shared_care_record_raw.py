# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_shared_care_record_raw.py
DESCRIPTION:
                Databricks notebook with code to append new raw data to historical
                data for the NHSX Analyticus unit metrics within the Shared Care Record topic.
                
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        19 Oct 2021
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake  beautifulsoup4 numpy urllib3 lxml dateparser regex openpyxl pyarrow==5.0.*

# COMMAND ----------

# Imports
# -------------------------------------------------------------------------
# Python:
import os
import io
import tempfile
from datetime import datetime
import json
import regex as re

# 3rd party:
import pandas as pd
import numpy as np
import openpyxl
from pathlib import Path
from azure.storage.filedatalake import DataLakeServiceClient

# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope="datalakefs", key="CONNECTION_STRING")

# COMMAND ----------

# MAGIC %run /Repos/dev/au-azure-databricks/functions/dbrks_helper_functions

# COMMAND ----------

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_shared_care_record_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
# file_system = config_JSON['pipeline']['adl_file_system']
# new_source_path = config_JSON['pipeline']['raw']['sink_path']
# new_source_file = config_JSON['pipeline']['raw']['sink_file']
# historical_source_path = config_JSON['pipeline']['raw']['appended_path']
# historical_source_file = config_JSON['pipeline']['raw']['appended_file']
# sink_path = config_JSON['pipeline']['raw']['appended_path']
# sink_file = config_JSON['pipeline']['raw']['appended_file']

# COMMAND ----------

file_system = "nhsxdatalakesagen2fsprod"
ods_table_path = "proc/projects/reference_tables/ods_codes/gp_mapping/"
ods_table_file = "table_odscodes_gp_mapping.parquet"

# COMMAND ----------

# Ingest ODS Code table 
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, ods_table_path)
file = datalake_download(CONNECTION_STRING, file_system, ods_table_path+latestFolder, ods_table_file)
df_ods = pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# COMMAND ----------

df_ods

# COMMAND ----------



# ODS Code STP table 
# -------------------------------------------------------------------------
df_STP = df[["PCN_STP_Code", "PCN_STP_Name"]].drop_duplicates().reset_index(drop = True)
df_STP_1 = df_STP.rename(columns = {'PCN_STP_Code': 'ODS STP code', 'PCN_STP_Name' : 'STP name'})
df_STP_1

# COMMAND ----------

source_path = 'land/sharepoint/Shared Documents/nhsx_au_ingestion/shcr/timestamp/xlsx/all_tables/2021-06-01/'
STP_file_name_list = datalake_listContents(CONNECTION_STRING, file_system, source_path)
file_types = ['.xlsx', '.XLSX']
STP_file_name_list_1 = [x for x in STP_file_name_list if any(c in x for c in file_types)]

# COMMAND ----------

STP_file_name_list_1

# COMMAND ----------

df = pd.DataFrame()
for filename in STP_file_name_list_1:
  file = datalake_download(CONNECTION_STRING, file_system, source_path, filename)
  new_df = pd.read_excel(file, sheet_name='STP', engine='openpyxl', index_col=None, header=None, skiprows=1)
  df = df.append(new_df)
df_1 = df.rename(columns = {0: "For Month",
                           1: "ODS STP Code",
                           2: "STP Name",
                           3: "ICS Name (if applicable)",
                           4: "ShCR Programme Name",
                           5: "Name of ShCR System",
                           6: "Number of users with access to the ShCR",
                           7: "Number of citizen records available to users via the ShCR",
                           8: "Number of ShCR views in the past month",
                           9: "Number of unique user ShCR views in the past month",
                           10: "Completed by (email)",
                           11: "Date completed:"})
df_2 = df_1.iloc[:, :12]
df_3 = df_2.dropna(how = 'all').reset_index(drop = True)

# COMMAND ----------

df_3

# COMMAND ----------


df_2

# COMMAND ----------



# my_filenames = [
#     os.path.join(root, name)
#     for root, dirs, files in os.walk(path)
#     for name in files
#     if name.endswith((".xlsx" or ".XLSX"))
# ]

# df = pd.DataFrame()
# for filename in my_filenames:
#     #print(filename)
#     new_df = pd.read_excel(filename, sheet_name='STP', engine='openpyxl', index_col=None, header=0)
#     df = df.append(new_df)
# df = df[df.columns.drop(list(df.filter(regex='Unnamed')))]
# df.to_csv('data/ShCR_STP_snapshot.csv', index=False)

# df = pd.DataFrame()
# for filename in my_filenames:
#     #print(filename)
#     new_df = pd.read_excel(filename, sheet_name='PCN', engine='openpyxl', index_col=None, header=0)
#     df = df.append(new_df)
# df = df[df.columns.drop(list(df.filter(regex='Unnamed')))]
# df.to_csv('data/ShCR_PCN_snapshot.csv', index=False)

# df = pd.DataFrame()
# for filename in my_filenames:
#     #print(filename)
#     new_df = pd.read_excel(filename, sheet_name='Trust', engine='openpyxl', index_col=None, header=0)
#     df = df.append(new_df)
# df = df[df.columns.drop(list(df.filter(regex='Unnamed')))]
# df.to_csv('data/ShCR_Trust_snapshot.csv', index=False)


# COMMAND ----------

# # Processing
# # -------------------------------------------------------------------------
# # Pull latest raw dataset
# latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, new_source_path)
# new_dataset = datalake_download(CONNECTION_STRING, file_system, new_source_path+latestFolder, new_source_file)
# fields = ['Practice_Code', 'Q114base', 'Q114_5', 'Q101base', 'Q101_4', 'q73base', 'Q73_1234base', 'q73_12' ]
# new_dataframe = pd.read_csv(io.BytesIO(new_dataset), usecols=fields)
# new_dataframe.rename(columns = {'Practice_Code': 'Practice code', 
#                            'Q114base': 'M090_denominator', 
#                            'Q114_5': 'M090_numerator',
#                            'Q101base': 'M091_denominator',
#                            'Q101_4': 'M091_numerator',
#                            'q73base': 'M092_denominator', 
#                            'Q73_1234base': 'M092_numerator_M093_denominator', 
#                            'q73_12': 'M093_numerator'},
#                            inplace=True)
# new_dataframe.insert(0,'Date','')
# new_dataframe.insert(0,'Collection end date','')
# new_dataframe.insert(0,'Collection start date','')
# date = '2021-01-01'
# new_dataframe["Date"] = date
# new_dataframe["Collection start date"] = "2021-01-01"
# new_dataframe["Collection end date"] = "2021-03-01"

# # pull historical dataset
# latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, historical_source_path)
# historical_dataset = datalake_download(CONNECTION_STRING, file_system, historical_source_path+latestFolder, historical_source_file)
# historical_dataframe = pd.read_parquet(io.BytesIO(historical_dataset), engine="pyarrow")

# COMMAND ----------

# # Append new data to historical data
# # -------------------------------------------------------------------------
# if date not in historical_dataframe["Date"].values:
#   historical_dataframe = historical_dataframe.append(new_dataframe)
#   historical_dataframe = historical_dataframe.reset_index(drop=True)
#   historical_dataframe.index.name = "Unique ID"
# else:
#   print("data already exists")

# COMMAND ----------

# # Upload processed data to datalake
# file_contents = io.BytesIO()
# historical_dataframe.to_parquet(file_contents, engine="pyarrow")
# datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
