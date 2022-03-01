# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_pomi_patient_enabled_month_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: % of patients enabled to manage appointments online (M043)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        01 Sept 2021
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
file_name_config = "config_pomi_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
file_system = config_JSON['pipeline']['adl_file_system']
sink_path = config_JSON['pipeline']['project']['databricks'][0]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][0]['sink_file']  

# COMMAND ----------

#Processing
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df4 = df[df["Field"] == "Pat_Appts_Enbld"]
df5 = df[df["Field"] == "patient_list_size"]
df4 = df4.groupby(["Report_Period_End", "Field"])
df6 = df4["Value"].aggregate(np.sum)
df6 = df6.reset_index()
df6["Report_Period_End"] = df6["Report_Period_End"].astype("datetime64[ns]")
df6 = df6.sort_values("Report_Period_End")
df6 = df6.reset_index(drop=True)
df5 = df5.groupby(["Report_Period_End", "Field"])
df7 = df5["Value"].aggregate(np.sum)
df7 = df7.reset_index()
df7["Report_Period_End"] = df7["Report_Period_End"].astype("datetime64[ns]")
df7 = df7.sort_values("Report_Period_End")
df7 = df7.reset_index(drop=True)
df8 = pd.merge(df6, df7, on="Report_Period_End", how="outer")
df8 = df8.drop(columns=["Field_x", "Field_y"])
df8["Percent of patients enabled to manage appointments online"] = (df8["Value_x"] / df8["Value_y"])
df8.rename(columns={
          "Report_Period_End": "Date",
          "Value_x": "Number of patients enabled to manage appointments online",
          "Value_y": "Total number of patients"},
          inplace=True,)
df8 = df8.round(4)
df8.index.name = "Unique ID"
df_processed = df8.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
