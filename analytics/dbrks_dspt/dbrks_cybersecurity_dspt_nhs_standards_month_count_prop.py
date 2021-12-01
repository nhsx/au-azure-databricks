# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           cybersecurity_dspt_nhs_standards_month_count_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: M020_M021  (Number and percent of Trusts, CSUs and CCGs registered for DSPT assessment, that meet or exceed the DSPT standard)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli, Chris Todd
CONTACT:        data@nhsx.nhs.uk
CREATED:        25 Nov 2021
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

#Download JSON config from Azure datalake
file_path_config = "/config/pipelines/nhsx-au-analytics/"
file_name_config = "config_dspt_nhs_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
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


DSPT_df = pd.read_csv(io.BytesIO(file))
ODS_code_df = pd.read_parquet(io.BytesIO(reference_file), engine="pyarrow")

date = datetime.now().strftime("%Y-%m-%d")
date_string = str(date)
DSPT_df['Code'] = DSPT_df['Code'].str.upper()
ODS_code_df['Close_Date'] = pd.to_datetime(ODS_code_df['Close_Date'], infer_datetime_format=True)
ODS_code_df['Open_Date'] =  pd.to_datetime(ODS_code_df['Open_Date'], infer_datetime_format=True)
DSPT_ODS = pd.merge(ODS_code_df, DSPT_df, how='outer', left_on="Code", right_on="Code")

DSPT_ODS =DSPT_ODS.reset_index(drop=True).rename(columns={"ODS_API_Role_Name": "Sector",})

close_date = datetime.strptime('2021-03-30 00:00:00', '%Y-%m-%d %H:%M:%S')
open_date = datetime.strptime('2021-03-30 00:00:00', '%Y-%m-%d %H:%M:%S')

DSPT_ODS_selection =  DSPT_ODS[(DSPT_ODS['Close_Date'].isna()) | (DSPT_ODS['Close_Date'] > close_date)]

DSPT_ODS_selection = DSPT_ODS_selection[
(DSPT_ODS_selection['Open_Date'] < open_date) & 
(DSPT_ODS_selection["Name"].str.contains("COMMISSIONING HUB")==False) &
(DSPT_ODS_selection["Code"].str.contains("RT4|RQF|RYT|0DH|0AD|0AP|0CC|0CG|0CH|0DG")==False)
].reset_index(drop=True)

df_filtered = DSPT_ODS_selection[
(DSPT_ODS_selection["Sector"] == "CLINICAL COMMISSIONING GROUP") |
(DSPT_ODS_selection["Sector"] == "COMMISSIONING SUPPORT UNIT") |
(DSPT_ODS_selection["Sector"] == "NHS TRUST")
].reset_index(drop=True)

df_count = df_filtered.groupby("Latest Status").size()
df_percent = (df_filtered.groupby("Latest Status").size() / len(df_filtered.index))
df = pd.concat([df_count, df_percent], axis=1).reset_index()

df.columns = ["Organisation Latest DSPT Status", "Count", "Percent of Total"]

df = df[
(df["Organisation Latest DSPT Status"] == "20/21 Standards Met") |
(df["Organisation Latest DSPT Status"] == "20/21 Standards Exceeded")
].reset_index(drop=True)

Total_Count = df["Count"].sum()
Total_Social_orgs = df_filtered[df_filtered.columns[0]].count()
Total_Percent = df["Percent of Total"].sum()

Data_f = [[
"Standards Met or Exceeded",
Total_Count,
Total_Social_orgs,
Total_Percent,
date_string,
]]

df_output = pd.DataFrame(Data_f,
    columns=[
        "DSPT status",
        "Number of Trusts, CSUs or CCGs with a standards met or exceeded DSPT status",
        "Total number of Trusts, CSUs or CCGs",
        "Percent of Trusts, CSUs or CCGs with a standards met or exceeded DSPT status",
        "Date",
    ],
)
df_output = df_output.round(4)
df_output.index.name = "Unique ID"

df_processed = df_output.copy()


# COMMAND ----------

df_processed

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
