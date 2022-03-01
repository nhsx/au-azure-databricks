# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_gp_patient_survey_results_easyuse_gp_website_year_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: % of patients reporting that their GP practice website was easy to use (M093)
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
sink_path = config_JSON['pipeline']['project']['databricks'][3]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][3]['sink_file']  

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
fields = ['Date', 'Practice code', 'M093_numerator', 'M092_numerator_M093_denominator']
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow", columns = fields)
df1 = df.rename(columns = {'M092_numerator_M093_denominator': 'Number of patients reporting having tried to use their GP practices website', 
                           'M093_numerator': 'Number of patients reporting that their GP practice website was easy to use'})
df1['Number of patients reporting having tried to use their GP practices website'].loc[df1['Number of patients reporting having tried to use their GP practices website'] < 0] = np.nan  
df1['Number of patients reporting that their GP practice website was easy to use'].loc[df1['Number of patients reporting that their GP practice website was easy to use'] < 0] = np.nan
df1['Percent of patients reporting that their GP practice website was easy to use'] = df1['Number of patients reporting that their GP practice website was easy to use']/df1['Number of patients reporting having tried to use their GP practices website']
df2 = df1.reset_index(drop = True)
df2.index.name = "Unique ID"
df_processed = df2.copy()

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
