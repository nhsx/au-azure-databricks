# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_toc_messages_provider_sent_admitted_patient_month_prop.py
DESCRIPTION:    Databricks notebook with processing code for the NHSX Analytics
                unit metric: Inpatient and day case ToC digital message utilisation (per 1,000 discharges) at a NHS Trust level (M030B.1)

CONTRIBUTORS:   Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        28 Feb. 2022
VERSION:        0.0.2
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
file_name_config = "config_toc_messages_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = config_JSON['pipeline']['adl_file_system']
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
denominator_source_path = config_JSON['pipeline']['project']['M30B_denominator_source_path']
denominator_source_file = config_JSON['pipeline']['project']['M30B_denominator_source_file']
sink_path = config_JSON['pipeline']['project']['databricks'][3]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][3]['sink_file']  

# COMMAND ----------

#Processing
#------------------------------------------
#Denominator data ingestion and processing
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, denominator_source_path)
file = datalake_download(CONNECTION_STRING, file_system, denominator_source_path+latestFolder, denominator_source_file)
df_denom = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_denom['Provider_Code'] = df_denom['Provider_Code'].str[:3]  #------ Only retain the first three characters of the NHS Trust Site ODS code, to equate it to the NHS Trust ODS code
df_denom_1 = df_denom.groupby([df_denom['Discharge_Date'].dt.strftime('%Y-%m'), 'Provider_Code'])['APC_Distcharges'].sum().reset_index()

#Numerator data ingestion and processing
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df1 = df[['_time', 'workflow','recipientOdsCode']]
df1 = df1[df1['workflow'].str.contains('TOC_FHIR_IP_DISCH_ACK')].reset_index(drop = True)
df1['Count'] = 1
df1['_time'] = pd.to_datetime(df1['_time']).dt.strftime("%Y-%m")
df2 = df1.groupby(['_time', 'workflow', 'recipientOdsCode']).sum().reset_index()
df2['Count'] = df2['Count'].div(2).apply(np.floor)
df3 = df2.set_index(['_time','recipientOdsCode','workflow']).unstack()['Count'].reset_index().fillna(0)
df4 = df3.rename(columns = {"TOC_FHIR_IP_DISCH_ACK": "Number of successful FHIR ToC acute admitted patient care discharge messages" })
df4['recipientOdsCode'] = df4['recipientOdsCode'].str[:3] #------ Only retain the first three characters of the NHS Trust Site ODS code, to equate it to the NHS Trust ODS code

#Joined data processing
df_join = pd.merge(df_denom_1, df4,  how='left', left_on=['Discharge_Date', 'Provider_Code'], right_on = ['_time', 'recipientOdsCode'])
df_join_1 = df_join.drop(columns = ['_time', 'recipientOdsCode']).rename(columns = {'Discharge_Date': 'Date',
                                                                                    'Provider_Code': 'Trust code',
                                                                                    'APC_Distcharges': 'Number of admitted patient care discharges (excluding mental health and maternity related discharges)'}).fillna(0)
df_join_1['Acute admitted patient care FHIR ToC utilisation (per 1,000 discharges)'] = df_join_1["Number of successful FHIR ToC acute admitted patient care discharge messages"]/ (df_join_1['Number of admitted patient care discharges (excluding mental health and maternity related discharges)']/1000)
df_join_2 = df_join_1.round(2)
df_join_2.index.name = "Unique ID"
df_processed = df_join_2.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
