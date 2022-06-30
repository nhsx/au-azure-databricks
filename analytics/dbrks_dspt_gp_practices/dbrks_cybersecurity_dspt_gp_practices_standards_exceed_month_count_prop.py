# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_cybersecurity_dspt_gp_practices_standards_exceed_month_count_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: M077B: No. and % of GP practices that exceed the DSPT standard (monthly snapshot).
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli, Chris Todd
CONTACT:        data@nhsx.nhs.uk
CREATED:        15 Feb. 2022
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

#Download JSON config from Azure datalake
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_dspt_gp_practices_snapshot_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = config_JSON['pipeline']['adl_file_system']
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_source_path = config_JSON['pipeline']['project']['reference_source_path']
reference_source_file = config_JSON['pipeline']['project']['reference_source_file']
sink_path = config_JSON['pipeline']['project']['databricks'][1]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][1]['sink_file']

# COMMAND ----------

# Ingestion and processing of numerator (DSPT status of GP practices)
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_csv(io.BytesIO(file))
df_1 = df[["Code", "Status"]]
df_1 = df_1.rename(columns = {'Status': 'Latest Status'})


# COMMAND ----------

# Ingestion and joining to reference deomintator data (NHS Digital: Number of registered GP Practices)
# ---------------------------------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_source_path)
file = datalake_download(CONNECTION_STRING, file_system, reference_source_path+latestFolder, reference_source_file)
df_ref = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_ref["EXTRACT_DATE"] = pd.to_datetime(df_ref["EXTRACT_DATE"])
df_ref_2 = df_ref.loc[df_ref['EXTRACT_DATE'] == df_ref['EXTRACT_DATE'].max()].reset_index(drop = True)


# Processing - merge denominator ("ground-truth for practices") and numerator ("DSPT status"). Left join (anything not found in denominator dropped.)
# ---------------------------------------------------------------------------------------------------------------------------------------------------
df_join = df_ref_2.merge(df_1,'left',left_on='PRACTICE_CODE', right_on='Code')

# COMMAND ----------

# Processing for joined tables
# -------------------------------------------------------------------------
df_join['Latest Status'] = df_join['Latest Status'].fillna('Not Published')
criteria = ['20/21 Standards Exceeded', '21/22 Standards Exceeded'] #-------- change this upon closure of the fiancial year. Please see the SOP.
def exceed_dspt(c):
    if c['Latest Status'] in criteria:
      return 1
    else:
      return 0
df_join['Number of GP practices that exceed the DSPT standard (snapshot)'] = df_join.apply(exceed_dspt, axis=1)
df_join.rename(columns={"PRACTICE_CODE":"Practice code", "FY":"Financial year"}, inplace = True)
df_join = df_join.drop(["Code", "Latest Status", "EXTRACT_DATE", "PRACTICE_NAME"], axis = 1)
df_join.insert(1,'Date', datetime.now().strftime('%Y-%m-%d'))
df_join.index.name = "Unique ID"
df_processed = df_join.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
