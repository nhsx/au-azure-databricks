# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_sandbox_notebook_1.py
DESCRIPTION:
                Databricks notebook to use as a sandbox for code development by analysts
USAGE:
                ...
CONTRIBUTORS:   Matia Ficarelli
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

# Connect to Azure datalake
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
source_path = 'test/source_data/firstname_lastname/data_description/'
source_file = 'test.csv'
sink_path = 'test/sink_data/firstname_lastname/data_description/'
sink_file = 'test.csv'

# COMMAND ----------

# Data ingestion
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_csv(io.BytesIO(file)) # ------ change depending on file type for:
                                   # ------ excel file: pd.read_excel(io.BytesIO(file), sheet_name = 'name_of_sheet', engine  = 'openpyxl') 
                                   # ------ parquet file: pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# COMMAND ----------

# Data Processing
# -------------------------------------------------------------------------

df

# COMMAND ----------

# Copy your final dataframe as renamed dataframe called 'df_processed'
# -------------------------------------------------------------------------
df_processed = df.copy() #------ replace 'df' with the name of your final 'df' i.e 'df_final' or 'df_7'

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
