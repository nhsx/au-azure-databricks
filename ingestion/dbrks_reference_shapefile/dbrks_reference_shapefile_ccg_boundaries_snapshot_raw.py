# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_reference_shapefile_ccg_boundaries_snapshot_raw.py
DESCRIPTION:
                Databricks notebook with code to ingest GeoJSON files from the ONS Geo Portal for CCG boundaries and create markdown change documentation
                for NHSX Analytics Unit dashboard projects
USAGE:         
CONTRIBUTORS:   Mattia Ficarelli and Craig Shenton
CONTACT:        data@nhsx.nhs.uk
CREATED:        15 Sept 2021
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
import regex as re

# 3rd party:
import pandas as pd
import numpy as np
from pathlib import Path
from azure.storage.filedatalake import DataLakeServiceClient
import requests
from urllib.request import urlopen
from urllib import request as urlreq
from bs4 import BeautifulSoup
import geojson
# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope="datalakefs", key="CONNECTION_STRING")

# COMMAND ----------

# MAGIC %run /Repos/prod/au-azure-databricks/functions/dbrks_helper_functions

# COMMAND ----------

# Load JSON config from Azure datalake
# -------------------------------------------------------------------------
file_path_config = "/config/pipelines/reference_tables/"
file_name_config = "config_shapefiles.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------

file_system = config_JSON['pipeline']['adl_file_system']
ods_source_path = config_JSON['pipeline']['raw']["source_path"]
ods_source_file = config_JSON['pipeline']['raw']["source_file"]
shapefile_sink_path = config_JSON['pipeline']['raw']['databricks'][0]["shapefile_sink_path"]
shapefile_sink_file = config_JSON['pipeline']['raw']['databricks'][0]["shapefile_sink_file"]
code_maping_sink_path = config_JSON['pipeline']['raw']['databricks'][0]["code_maping_sink_path"]
code_maping_sink_file = config_JSON['pipeline']['raw']['databricks'][0]["code_maping_sink_file"]
markdown_sink_path = config_JSON['pipeline']['raw']['databricks'][0]["markdown_sink_path"]
markdown_sink_file = config_JSON['pipeline']['raw']['databricks'][0]["markdown_sink_file"]


# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
# Ingest CCG boundary GeoJSON

search_url = "https://ons-inspire.esriuk.com/arcgis/rest/services/Health_Boundaries/"
url_start = "https://ons-inspire.esriuk.com"
string_filter = "Clinical_Commissioning_Groups_April"
ons_geoportal_geojson = ons_geoportal_file_download(search_url, url_start, string_filter)

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
#Ingest CCG ONS to ODS code mapping table, and map to CCG dataframe generated from the CCG boundary GeoJSON

column_ons_code = ons_geoportal_geojson['fields'][1]['name'].lower()
column_ccg_name = ons_geoportal_geojson['fields'][2]['name'].lower()

search_url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
url_start = "https://services1.arcgis.com"
string_filter = "CCG_APR"
ccg_code_map_json = ons_geoportal_file_download(search_url, url_start, string_filter)
ccg_code_map_df = pd.json_normalize(ccg_code_map_json['features'])

#Define column title variables 
column_ods_code = ccg_code_map_json['fields'][1]['name'].lower()
column_ons_code_1 = ccg_code_map_json['fields'][0]['name'].lower()

#Extract ONS, ODS codes and organization names from ONS to ODS code mapping file
ccg_code_map_df = ccg_code_map_df.iloc[:,:2]
ccg_code_map_df.columns = ccg_code_map_df.columns.str.lower()
ccg_code_map_df.rename(columns={'attributes.%s' %column_ons_code_1 :'ONS CCG code', 'attributes.%s' %column_ods_code: 'ODS CCG code'}, inplace=True)

#Extract ONS codes and organization names from shapefile
ccg_geojson_df = pd.json_normalize(ons_geoportal_geojson['features'])
ccg_geojson_df.columns = ccg_geojson_df.columns.str.lower()
ccg_geojson_df_1 = ccg_geojson_df.iloc[:,1:3]
ccg_geojson_df_1.rename(columns={'attributes.%s' %column_ons_code :'ONS CCG code', 'attributes.%s' %column_ccg_name: 'CCG name'}, inplace=True)

#Merge files to create a mapping table
mapped_ccg_geojson_df = pd.merge(ccg_geojson_df_1, ccg_code_map_df, on=['ONS CCG code', 'ONS CCG code'])
mapped_ccg_geojson_df.index.name = "Unique ID"

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
#Ingest ODS Code table, filtered for CCGs, and generate a difference markdown file from the 

latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, ods_source_path)
ods_codes_table = datalake_download(CONNECTION_STRING, file_system, ods_source_path+latestFolder, ods_source_file)
ods_df = pd.read_parquet(io.BytesIO(ods_codes_table), engine="pyarrow")
ods_df_1 = ods_df[ods_df['ODS_API_Role_Name']=='CLINICAL COMMISSIONING GROUP']
ods_df_2 = ods_df_1[ods_df_1['Name'].str.contains('COMMISSIONING HUB')==False]
ods_df_3 = ods_df_2[['Code','Name','Open_Date', 'Close_Date']]
ods_df_3.rename(columns={'Code':'ODS CCG code', 'Name': 'CCG name (ODS API Database)', 'Open_Date': 'Open date', 'Close_Date': 'Close date'}, inplace=True)
ods_mappped_df = pd.merge(mapped_ccg_geojson_df, ods_df_3, on='ODS CCG code',how='outer')

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
#CCG boundary GeoJSON
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
geojson.dump(ons_geoportal_geojson, file_contents, ensure_ascii=False, indent=4)
datalake_upload(file_contents, CONNECTION_STRING, file_system, shapefile_sink_path+current_date_path, shapefile_sink_file)

#CCG ONS to ODS code mapping table
file_contents = io.BytesIO()
mapped_ccg_geojson_df.to_parquet(file_contents, engine="pyarrow")
datalake_upload(file_contents, CONNECTION_STRING, file_system, code_maping_sink_path+current_date_path, code_maping_sink_file)

#CCG ODS code mapping to CCG shapefile documentation
file_contents = io.StringIO()
ods_mappped_df.to_markdown(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, markdown_sink_path+current_date_path, markdown_sink_file)
