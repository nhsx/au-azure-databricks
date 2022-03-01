# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_eps_usage_patient_nominated_pharmacy_month_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: % patients with nominated pharmacy (M083)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        04 October 2021
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
file_name_config = "config_gp_eps_dbrks.json"
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

#Processing
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df1 = df[['ODS Code', 'GP Practice (ODS Code)', 'Registered Patients', '% with nominated pharm', 'Date']]
df1['% with nominated pharm'] = df1['% with nominated pharm'].str.replace("%", "")
df1['% with nominated pharm'] = (pd.to_numeric(df1['% with nominated pharm'])/100)
df1['Number of registered patients with a nominated pharmacy'] = (df1['% with nominated pharm'] * pd.to_numeric(df1['Registered Patients'])).round()
df2 = df1[['ODS Code', 'Number of registered patients with a nominated pharmacy', 'Registered Patients', '% with nominated pharm', 'Date']]	
df2.rename(columns = {'ODS Code': 'Practice code', 'Registered Patients': 'Number of registered patients', '% with nominated pharm':'% of registered patients with a nominated pharmacy'}, inplace=True)
df2.index.name = "Unique ID"
df_processed = df2.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
