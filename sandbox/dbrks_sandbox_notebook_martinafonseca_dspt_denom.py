# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_sandbox_notebook_martinafonseca_dspt_denom.py
DESCRIPTION:
                Databricks notebook to use as a sandbox for code development for calculating the % of GP practices that meet or exceed DSPT standard
USAGE:
                ...
CONTRIBUTORS:   Martina Fonseca
CONTACT:        data@nhsx.nhs.uk
CREATED:        10 Feb 2022
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

# Connect to Azure datalake - do not change
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope="datalakefs", key="CONNECTION_STRING")

# COMMAND ----------

# MAGIC %run /Repos/dev/au-azure-databricks/functions/dbrks_helper_functions

# COMMAND ----------

# Source Path and File, Source Path and File, Datalake Container
# ---------------------------------------------------------------

# Do not change
# ---------------------------------------------------------------
file_system = 'nhsxdatalakesagen2fsprod'

# Change to point to the path of your data 
# ---------------------------------------------------------------
source_path = 'test/source_data/martina_fonseca/dspt_gp_denom/'
source_file = 'gp-reg-pat-prac-mar19.csv'
sink_path = 'test/sink_data/martina_fonseca/dspt_gp_denom/'
sink_file = 'processed.csv'

# COMMAND ----------

# Data ingestion
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_csv(io.BytesIO(file)) # ------ change depending on file type for:
                                   # ------ excel file: pd.read_excel(io.BytesIO(file), sheet_name = 'name_of_sheet', engine  = 'openpyxl') 
                                   # ------ parquet file: pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# COMMAND ----------

latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file_name_list = datalake_listContents(CONNECTION_STRING, file_system, source_path+latestFolder)
file_name_list = [file for file in file_name_list if '.csv' in file]

# COMMAND ----------

file_name_list

# COMMAND ----------

dataset_concat=pd.DataFrame()
for new_source_file in file_name_list:
  new_file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, new_source_file)
  new_df = pd.read_csv(io.BytesIO(new_file))
  dataset_concat=dataset_concat.append(new_df)

# COMMAND ----------

def fy(entry): # could finesse and only allow FY to generate for March, precludes a bit multiple downloads in same FY being used for denominator
  if entry['date'].month > 3:
    return str(entry['date'].year)+"/"+str(entry['date'].year+1)
  else:
    return str(entry['date'].year-1)+"/"+str(entry['date'].year)

# COMMAND ----------

# Data Processing
# -------------------------------------------------------------------------

# Add financial year as variable. By adding this, we can do a merge on financial year with DSPT data. SOP: take care to only include one cut per financial year (March of FY) in the folder
format_str='%d%b%Y' # old format
format_str_new='%Y-%m-%d' # from 2021 format changed...
dataset_concat['EXTRACT_DATE']

dataset_concat['date'] = pd.to_datetime(dataset_concat['EXTRACT_DATE'],format=format_str,errors="coerce").fillna(pd.to_datetime(dataset_concat['EXTRACT_DATE'],format=format_str_new,errors="coerce"))

#dataset_concat['FY'] = dataset_concat.apply(lambda row: fy(row),axis=1) # create a string for FY that matches the one in numerator
dataset_concat['FY'] = dataset_concat.apply(fy,axis=1) 
#datetime.strptime('01MAR2020',format_str)

# only keep minimal columns
col_keep = ['PRACTICE_CODE','PRACTICE_NAME','EXTRACT_DATE','FY']
dataset_concat = dataset_concat[col_keep]

dataset_concat.reset_index()
dataset_concat

# COMMAND ----------

# Copy your final dataframe as renamed dataframe called 'df_processed'
# -------------------------------------------------------------------------
df_processed = dataset_concat.copy() #------ replace 'df' with the name of your final 'df' i.e 'df_final' or 'df_7'
df_processed

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
df_processed.to_csv(file_contents,index=False) # added index=False
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
