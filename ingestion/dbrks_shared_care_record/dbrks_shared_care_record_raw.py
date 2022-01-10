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
file_system = config_JSON['pipeline']['adl_file_system']
source_path = config_JSON['pipeline']['raw']['source_path']
sink_path = config_JSON['pipeline']['raw']['sink_path']

# COMMAND ----------

latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, file_path)
latestFolder

# COMMAND ----------

# Get list of all files in latest folder
def datalake_listDirectory(CONNECTION_STRING, file_system, source_path):
    try:
        service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
        file_system_client = service_client.get_file_system_client(file_system=file_system)
        paths = file_system_client.get_paths(path=source_path)
        directory = []
        for path in paths:
            path.name = path.name.replace(source_path, '')
            directory.append(path.name)
    except Exception as e:
        print(e)
    return directory, paths
  
directory, paths = datalake_listDirectory(CONNECTION_STRING, file_system, file_path+latestFolder)
directory

# COMMAND ----------

from openpyxl import load_workbook

def get_sheetnames_xlsx(filepath):
    wb = load_workbook(filepath, read_only=True, keep_links=False)
    return wb.sheetnames
  
stp_df = pd.DataFrame()
trust_df = pd.DataFrame()
pcn_df = pd.DataFrame()
other_df = pd.DataFrame()

for filename in directory:
    file = datalake_download(CONNECTION_STRING, file_system, file_path+latestFolder, filename)
    sheets = get_sheetnames_xlsx(io.BytesIO(file))
    
    # STP calculations
    sheet_name = [sheet for sheet in sheets if sheet.startswith('STP')]
    xls_file = pd.read_excel(io.BytesIO(file), sheet_name=sheet_name, engine='openpyxl')
    for key in xls_file:
        xls_file[key].drop(list(xls_file[key].filter(regex = 'Unnamed:')), axis = 1, inplace = True)
        xls_file[key] = xls_file[key].loc[~xls_file[key]["For Month"].isnull()]
        # get excel file metadata
        STP_code = xls_file[key]["ODS STP Code"].unique()[0]              # get stp code for all sheets
        STP_name = xls_file[key]["STP Name"].unique()[0]                  # get stp name for all sheets
        ICS_name = xls_file[key]["ICS Name (if applicable)"].unique()[0]  # get ics name for all sheets
        # append to dataframe
        stp_df = stp_df.append(xls_file[key], ignore_index=True)
        
    # Trust calculations
    sheet_name = [sheet for sheet in sheets if sheet.startswith('Trust')]
    xls_file = pd.read_excel(io.BytesIO(file), sheet_name=sheet_name, engine='openpyxl')
    for key in xls_file:
        xls_file[key].drop(list(xls_file[key].filter(regex = 'Unnamed:')), axis = 1, inplace = True)
        xls_file[key] = xls_file[key].loc[~xls_file[key]["For Month"].isnull()]
        xls_file[key].insert(1, "ODS STP Code", STP_code, False)
        xls_file[key].insert(2, "STP Name", STP_name, False)
        xls_file[key].insert(3, "ICS Name (if applicable)", ICS_name, False)
        trust_df = trust_df.append(xls_file[key], ignore_index=True)
        
    # PCN calculations
    sheet_name = [sheet for sheet in sheets if sheet.startswith('PCN')]
    xls_file = pd.read_excel(io.BytesIO(file), sheet_name=sheet_name, engine='openpyxl')
    for key in xls_file:
        xls_file[key].drop(list(xls_file[key].filter(regex = 'Unnamed:')), axis = 1, inplace = True)
        xls_file[key] = xls_file[key].loc[~xls_file[key]["For Month"].isnull()]
        xls_file[key].insert(1, "ODS STP Code", STP_code, False)
        xls_file[key].insert(2, "STP Name", STP_name, False)
        xls_file[key].insert(3, "ICS Name (if applicable)", ICS_name, False)
        pcn_df = pcn_df.append(xls_file[key], ignore_index=True)
        
    # Other calculations
    sheet_name = [sheet for sheet in sheets if sheet.startswith('Other')]
    xls_file = pd.read_excel(io.BytesIO(file), sheet_name=sheet_name, engine='openpyxl')
    for key in xls_file:
        xls_file[key].drop(list(xls_file[key].filter(regex = 'Unnamed:')), axis = 1, inplace = True)
        xls_file[key] = xls_file[key].loc[~xls_file[key]["For Month"].isnull()]
        #xls_file[key].insert(1, "ODS STP Code", STP_code, False)
        #xls_file[key].insert(2, "STP Name", STP_name, False)
        #xls_file[key].insert(3, "ICS Name (if applicable)", ICS_name, False)
        other_df = other_df.append(xls_file[key], ignore_index=True)

# COMMAND ----------

trust_df.head()

# COMMAND ----------

# Upload appended data to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.BytesIO()
STP_df.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
