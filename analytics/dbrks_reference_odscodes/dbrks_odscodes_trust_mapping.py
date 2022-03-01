# Databricks notebook source
# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_odscodes_trust_mapping.py
DESCRIPTION:
                Databricks notebook with processing code for the reference table which maps Trusts to ICBs and NHS Regions for NHSX Analytics Unit dashboard projects
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        24 Feb. 2022
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
file_path_config = "/config/pipelines/reference_tables/"
file_name_config = "config_odscodes_trust.json"
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
df_1 = df[["Organisation_Code", "Organisation_Name", "Region_Code", "Region_Name", "STP_Code", "STP_Name", "Effective_To"]]
df_1["Effective_To"] = pd.to_datetime(df_1["Effective_To"])
df_2 = df_1[~df_1['Organisation_Code'].isna()]
df_3 = df_2[df_2['Effective_To'].isna()].reset_index(drop = True).drop("Effective_To", axis = 1)
df_processed = df_3.copy()

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.BytesIO()
df_processed.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
