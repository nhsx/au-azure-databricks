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
#DSPT_df = pd.read_csv(tempFilePath + path + file_name)
#ODS_code_df = pd.read_parquet(tempFilePath_ODS + "/" + latest_folder_ODS + "/" + file_name_ODS, engine = 'pyarrow')
DSPT_df['Code'] = DSPT_df['Code'].str.upper()
ODS_code_df['Close_Date'] = pd.to_datetime(ODS_code_df['Close_Date'], infer_datetime_format=True)
ODS_code_df['Open_Date'] =  pd.to_datetime(ODS_code_df['Open_Date'], infer_datetime_format=True)
DSPT_ODS = pd.merge(ODS_code_df, DSPT_df, how='outer', left_on="Code", right_on="Code")
DSPT_ODS = (
    DSPT_ODS.drop(
        [
            "Address_Line_1",
            "Address_Line_2",
            "Address_Line_3",
            "Address_Line_4",
            "Address_Line_5",
            "Postcode",
            "Import_Date",
            "Created_Date",
            "Commissioner (From ODS)",
            "ODS_API_Role_Code",
            "Source",
            "Char_8_ASCII_Index",
            "PK_NonStaticID",
            "Organisation Name",
            "Primary Sector"
        ],
        1,
    )
    .reset_index(drop=True)
    .rename(
        columns={
            "ODS_API_Role_Name": "Sector",
        }
    )
)

DSPT_ODS["count"] = 1
close_date = datetime.strptime('2021-03-30 00:00:00', '%Y-%m-%d %H:%M:%S')
open_date = datetime.strptime('2021-03-30 00:00:00', '%Y-%m-%d %H:%M:%S')
DSPT_ODS_selection =  DSPT_ODS[DSPT_ODS['Close_Date'].isna()]
DSPT_ODS_selection_2 =  DSPT_ODS[DSPT_ODS['Close_Date'] > close_date]
DSPT_ODS_selection_3 = pd.concat([DSPT_ODS_selection, DSPT_ODS_selection_2])
DSPT_ODS_selection_4 = DSPT_ODS_selection_3[DSPT_ODS_selection_3['Open_Date'] < open_date]
DSPT_ODS_selection_5 = DSPT_ODS_selection_4[DSPT_ODS_selection_4["Name"].str.contains("COMMISSIONING HUB")==False]
DSPT_ODS_selection_6 = DSPT_ODS_selection_5[DSPT_ODS_selection_5["Code"].str.contains("RT4|RQF|RYT|0DH|0AD|0AP|0CC|0CG|0CH|0DG")==False]
DSPT_ODS_selection_7 = DSPT_ODS_selection_6.reset_index(drop=True)
df_filtered_1 = DSPT_ODS_selection_7[DSPT_ODS_selection_7["Sector"] == "CLINICAL COMMISSIONING GROUP"]
df_filtered_2 = DSPT_ODS_selection_7[DSPT_ODS_selection_7["Sector"] == "COMMISSIONING SUPPORT UNIT"]
df_filtered_3 = DSPT_ODS_selection_7[DSPT_ODS_selection_7["Sector"] == "NHS TRUST"]
df_filtered_4 = pd.concat([df_filtered_1, df_filtered_2, df_filtered_3])
df_filtered_4 = df_filtered_4.reset_index(drop=True)
df1 = df_filtered_4.groupby("Latest Status")["count"].sum()
df_percent = (
    df_filtered_4.groupby("Latest Status")["count"].sum() / df_filtered_4["count"].sum()
)
df2 = pd.concat([df1, df_percent], axis=1)
df2 = df2.reset_index()
df2.columns = ["Organisation Latest DSPT Status", "Count", "Percent of Total"]
df3 = df2[df2["Organisation Latest DSPT Status"] == "20/21 Standards Met"]
df4 = df2[df2["Organisation Latest DSPT Status"] == "20/21 Standards Exceeded"]
df5 = pd.concat([df3, df4])
df5 = df5.reset_index(drop=True)
Total_Count = df5["Count"].sum()
Total_Social_orgs = df_filtered_4[df_filtered_4.columns[0]].count()
Total_Percent = df5["Percent of Total"].sum()
Data_f = [
    [
        "Standards Met or Exceeded",
        Total_Count,
        Total_Social_orgs,
        Total_Percent,
        date_string,
    ]
]
df6 = pd.DataFrame(
    Data_f,
    columns=[
        "DSPT status",
        "Number of Trusts, CSUs or CCGs with a standards met or exceeded DSPT status",
        "Total number of Trusts, CSUs or CCGs",
        "Percent of Trusts, CSUs or CCGs with a standards met or exceeded DSPT status",
        "Date",
    ],
)
df6 = df6.round(4)
df6.index.name = "Unique ID"

df_processed = df6.copy()


# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
