# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_toc_messages_data_to_send.py
DESCRIPTION:
                Databricks notebook with processing code to genrate a processed xlsx file with data on the No. transfer of care digital messages sent to GPs (all use cases) (M030A)
                and the No. transfer of care digital messages sent to GPs per 1,000 discharges (inpatient and mental health) (M030B) to send to the NHSX Digital Standards team
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        01 Nov 2021
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* xlsxwriter

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
import xlsxwriter
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
sink_path = config_JSON['pipeline']['project']['databricks'][6]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][6]['sink_file']

# COMMAND ----------

#Processing No. transfer of care digital messages sent to GPs (all use cases) (M030A)
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df1 = df[['_time', 'workflow', 'senderOdsCode', 'recipientOdsCode']]
df1 = df1[df1['workflow'].str.contains('ACK')].reset_index(drop = True)
df1['Count'] = 1
df1['_time'] = pd.to_datetime(df1['_time']).dt.strftime("%Y-%m")
df2 = df1.groupby(['_time', 'workflow', 'senderOdsCode', 'recipientOdsCode']).sum().reset_index()
df2['Count'] = df2['Count'].div(2).apply(np.floor)
df2 = df2.drop(columns = ["senderOdsCode", "recipientOdsCode"]).groupby(["workflow", "_time"]).sum().reset_index()
df3 = df2.set_index(['_time','workflow']).unstack()['Count'].reset_index().fillna(0)
df4 = df3.rename(columns = {'_time': 'Date', 
                            'TOC_FHIR_EC_DISCH_ACK': 'Number of successful FHIR ToC emergency care discharge messages',
                            'TOC_FHIR_IP_DISCH_ACK': 'Number of successful FHIR ToC acute admitted patient care discharge messages',
                            'TOC_FHIR_MH_DISCH_ACK': 'Number of successful FHIR ToC mental health discharge messages',
                            'TOC_FHIR_OP_ATTEN_ACK': 'Number of successful FHIR ToC outpatient clinic attendance messages'})
df4.columns.name = None
df4.index.name = "Unique ID"
if df4['Date'].iloc[-1] == datetime.now().strftime("%Y-%m"):
  df4.drop(df4.tail(1).index,inplace=True)
df_M030A = df4.copy()

# COMMAND ----------

#No. transfer of care digital messages sent to GPs per 1,000 discharges (inpatient and mental health) (M030B) 

#Denominator data ingestion and processing
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, denominator_source_path)
file = datalake_download(CONNECTION_STRING, file_system, denominator_source_path+latestFolder, denominator_source_file)
df_denom = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df_denom_1 = df_denom.groupby(df_denom['Discharge_Date'].dt.strftime('%Y-%m'))['APC_Distcharges'].sum().reset_index()

#Numerator data ingestion and processing
df5 = df3.copy()
df5["Number of successful FHIR ToC acute admitted patient care discharge messages"] = df5["TOC_FHIR_IP_DISCH_ACK"] 
df6 = df5.drop(columns=['TOC_FHIR_IP_DISCH_ACK', 'TOC_FHIR_OP_ATTEN_ACK', 'TOC_FHIR_EC_DISCH_ACK', 'TOC_FHIR_MH_DISCH_ACK'])

#Joined data processing
df_join = df_denom_1.join(df6, how='left', lsuffix='Discharge_Date', rsuffix='_time')
df_join_1 = df_join.drop(columns = ['_time']).rename(columns = {'Discharge_Date': 'Date', 'APC_Distcharges': 'Number of admitted patient care discharges (excluding mental health and maternity related discharges)'})
df_join_1['Acute admitted patient care FHIR ToC utilisation (per 1,000 discharges)'] = df_join_1["Number of successful FHIR ToC acute admitted patient care discharge messages"]/ (df_join_1['Number of admitted patient care discharges (excluding mental health and maternity related discharges)']/1000)
df_join_2 = df_join_1.round(2)
df_join_2.index.name = "Unique ID"
df_M030B = df_join_2.copy()

# COMMAND ----------

#Add link to cover sheet
d = {'Cover Sheet': ['For more information on the data within this file refer to cover_sheet_toc_messages_v001']}
df_cover = pd.DataFrame(data = d)
df_cover['Cover Sheet Link'] = '=HYPERLINK("https://docs.google.com/spreadsheets/d/1NN21Gt2ejSoQ9KMtedra3Ve5BBFwLVm0/edit#gid=821869811", "Link")'

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.BytesIO()
with pd.ExcelWriter(file_contents) as writer:
  df_cover.to_excel(writer, sheet_name='Cover sheet', index=False)
  df_M030A.to_excel(writer, sheet_name='ToC messages count', index=False)
  df_M030B.to_excel(writer, sheet_name='ToC messages proportion', index=False)
  
  # formatting excel file to add in text wrapping
  workbook=writer.book
  worksheet_1 = writer.sheets['Cover sheet']
  worksheet_2 = writer.sheets['ToC messages count']
  worksheet_3 = writer.sheets['ToC messages proportion']
  format = workbook.add_format({'text_wrap': True, 'align': 'center'})
  worksheet_1.set_column('A:E', 50, format)
  worksheet_2.set_column('A:E', 70, format)
  worksheet_3.set_column('A:E', 70, format)

datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
