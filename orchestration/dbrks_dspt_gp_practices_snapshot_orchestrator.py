# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_dspt_gp_practices_snapshot_orchestrator.py
DESCRIPTION:
                Orchestrator databricks notebook which runs the processing notebooks for NHSX Analyticus unit metrics within the DSPT gp practices topic (monthly snapshot)
USAGE:
                ...
CONTRIBUTORS:   Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        16 Feb. 2022
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

#Squentially run metric notebooks
for index, item in enumerate(config_JSON['pipeline']['project']['databricks']): # get index of objects in JSON array
  try:
    notebook = config_JSON['pipeline']['project']['databricks'][index]['databricks_notebook']
    dbutils.notebook.run(notebook, 1000) #1000 sec timeout
  except Exception as e:
    print(e)
    raise Exception()
