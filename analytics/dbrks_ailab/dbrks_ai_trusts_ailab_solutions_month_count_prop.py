# Databricks notebook source

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:          dbrks_ai_trusts_ailab_solutions_month_count_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: Number and percent of Trusts with at least one NHS AI Lab solution trialled (AI Lab) (M022_M023)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli ,Everistus Oputa
CONTACT:        data@nhsx.nhs.uk
CREATED:        29 Nov 2021
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
file_name_config = "config_ailab_solutions_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
reference_path = config_JSON['pipeline']['project']['reference_path']
reference_file = config_JSON['pipeline']['project']['reference_file']
file_system = config_JSON['pipeline']['adl_file_system']
sink_path = config_JSON['pipeline']['project']['sink_path']
sink_file = config_JSON['pipeline']['project']['sink_file']  

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
reference_latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, reference_path)

file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
reference_file = datalake_download(CONNECTION_STRING, file_system, reference_path+reference_latestFolder, reference_file)


df = pd.read_csv(io.BytesIO(file))
ODS_code_df = pd.read_parquet(io.BytesIO(reference_file), engine="pyarrow")

df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
df1 = df[["Organisation_Code", "Trust_PilotFlag", "Trust_Live", "Last_Refreshed"]]
ailab_ODS = pd.merge(df1, ODS_code_df, left_on="Organisation_Code", right_on="Code")
ailab_ODS = (ailab_ODS.drop(
            [
                "Address_Line_1",
                "Address_Line_2",
                "Address_Line_3",
                "Address_Line_4",
                "Address_Line_5",
                "Postcode",
                "Open_Date",
                "Close_Date",
                "Import_Date",
                "Created_Date",
            ],
            1,
        )
        .reset_index(drop=True)
        .rename(
            columns={
                "Date Of Publication": "Date Of DSPT Publication",
            }
        )
    )
df2 = ailab_ODS[["Trust_PilotFlag", "Trust_Live", "Last_Refreshed"]]
df2 = df2.reset_index(drop=True)
No_Trusts_AI = df2["Trust_PilotFlag"].sum()
Total_Trusts = df2["Trust_Live"].sum()
Percent_Trusts_AI = No_Trusts_AI / Total_Trusts
Date = df2["Last_Refreshed"].max()

Data_f = [[No_Trusts_AI, Total_Trusts, Percent_Trusts_AI, Date]]
df3 = pd.DataFrame(
        Data_f,
        columns=[
            "Number of Trusts with at least one NHS AI Lab solution trialled",
            "Total number of Trusts",
            "Percent of Trusts with at least one NHS AI Lab solution trialled",
            "Data updated",
        ],
    )
df3 = df3.round(4)
df3 = df3.reset_index(drop=True)
df3.index.name = "Unique ID"
df_processed = df3.copy()

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
