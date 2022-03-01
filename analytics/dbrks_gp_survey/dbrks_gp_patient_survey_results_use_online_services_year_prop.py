# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_gp_patient_survey_results_use_online_services_year_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: % of patients reporting using one or more of the GP practice online services (M090)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        02 Sept 2021
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
file_name_config = "config_gp_patient_survey_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
file_system = config_JSON['pipeline']['adl_file_system']
sink_path = config_JSON['pipeline']['project']['databricks'][1]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][1]['sink_file']  

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
fields = ['Date', 'Practice code', 'M090_denominator', 'M090_numerator']
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow", columns = fields)
df1 = df.rename(columns = {'M090_denominator': 'Total number of responses', 'M090_numerator': 'Number of patients reporting not using GP practice online services'})
df1['Number of patients reporting not using GP practice online services'].loc[df1['Number of patients reporting not using GP practice online services'] < 0] = np.nan 
df1['Number of patients reporting using GP practice online services'] = df1['Total number of responses'] - df1['Number of patients reporting not using GP practice online services']
df1['Percent of patients reporting using GP practice online services'] = df1['Number of patients reporting using GP practice online services']/df1['Total number of responses']
df2 = df1.drop(columns = ["Number of patients reporting not using GP practice online services"])
df3 = df2.reset_index(drop = True)
df3.index.name = "Unique ID"
df_processed = df3.copy()

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
