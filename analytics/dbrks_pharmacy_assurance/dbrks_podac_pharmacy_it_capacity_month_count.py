# Databricks notebook source
# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:          dbrks_podac_pharmacy_it_capacity_month_count.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: ncrease in capacity of Pharmacy IT systems to enable pharmacists to provide consultation services to patients (no.) (PODAC) (M019)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli ,Everistus Oputa
CONTACT:        data@nhsx.nhs.uk
CREATED:        02 Dec 2021
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

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_pharmacy_assurance_dbrks.json"
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
df = pd.read_csv(io.BytesIO(file))
df.insert(loc=4, column="Count", value=1)
df1 = df[~df["CPCS type"].isin(["Minor Illness Referral Consultation"])]
df1 = df1.reset_index(drop=True)
Total_Count = df1["Count"].sum()
df2 = df1.groupby(["System Assured"])[["Count"]].count()
df2 = df2.reset_index()
df2.rename(
columns={
            "System Assured": "Pharmacy IT system assurance status",
            "Count": "Number of pharmacies",
},
inplace=True,)
df2["Pharmacy IT system assurance status"].replace(False,'Not assured', inplace=True)
df2["Pharmacy IT system assurance status"].replace(True,'Assured', inplace=True)
df2.loc[-1] = ["Total", Total_Count]
df3 = df2.reset_index(drop=True)
df3["Date"] = df["Date"].max()
df3.index.name = "Unique ID"
df_processed = df3.copy()

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
