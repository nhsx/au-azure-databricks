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
                Databricks notebook with code to ingest GeoJSON files from the ONS Geo Portal for NHS regions and create markdown change documentation
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
import urllib
from urllib.request import urlopen
from urllib import request as urlreq
from bs4 import BeautifulSoup
import geojson
# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope="datalakefs", key="CONNECTION_STRING")

# COMMAND ----------

# MAGIC %run /Repos/dev/au-azure-databricks/functions/dbrks_helper_functions

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
shapefile_sink_path = config_JSON['pipeline']['raw']['databricks'][2]["shapefile_sink_path"]
shapefile_sink_file = config_JSON['pipeline']['raw']['databricks'][2]["shapefile_sink_file"]
code_maping_sink_path = config_JSON['pipeline']['raw']['databricks'][2]["code_maping_sink_path"]
code_maping_sink_file = config_JSON['pipeline']['raw']['databricks'][2]["code_maping_sink_file"]
markdown_sink_path = config_JSON['pipeline']['raw']['databricks'][2]["markdown_sink_path"]
markdown_sink_file = config_JSON['pipeline']['raw']['databricks'][2]["markdown_sink_file"]

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
#Ingest NHS region boundary GeoJSON

search_url = "https://ons-inspire.esriuk.com/arcgis/rest/services/Health_Boundaries/"
url_1 = "https://ons-inspire.esriuk.com"
url_2 = '/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'

page = requests.get(search_url)
response = urlreq.urlopen(search_url)
soup = BeautifulSoup(response.read(), "lxml")
nhs_regions_url = soup.find_all('a', href=re.compile("NHS_England_Regions"))[-1].get('href')
full_url = url_1 + nhs_regions_url + url_2
with urlopen(full_url)  as response:
    nhs_regions_geojson = json.load(response)



# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
#Ingest NHS region ONS to ODS code mapping table, and map to NHS region dataframe generated from the CCG boundary GeoJSON

column_ons_code = nhs_regions_geojson['fields'][1]['name'].lower()
column_region_name = nhs_regions_geojson['fields'][2]['name'].lower()

search_url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
url_1 = "https://services1.arcgis.com"
url_2 = '/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
page = requests.get(search_url)
response = urlreq.urlopen(search_url)
soup = BeautifulSoup(response.read(), "lxml")
region_code_map_url = soup.find_all('a', href=re.compile("NHSER_APR"))[-1].get('href')
full_url = url_1 + region_code_map_url + url_2
with urlopen(full_url) as response:
    region_code_map_json = json.load(response)
    region_code_map_df = pd.json_normalize(region_code_map_json['features'])

column_ons_code_1 = region_code_map_json['fields'][0]['name'].lower()
column_ods_code = region_code_map_json['fields'][1]['name'].lower()
column_region_name = region_code_map_json['fields'][2]['name'].lower()
region_code_map_df = region_code_map_df.iloc[:,:3]
region_code_map_df.columns = region_code_map_df.columns.str.lower()
region_code_map_df.rename(columns={'attributes.%s' %column_ons_code_1 :'ONS NHS region code', 'attributes.%s' %column_ods_code: 'ODS NHS region code', 'attributes.%s' %column_region_name: 'NHS region name'}, inplace=True)
region_geojson_df = pd.json_normalize(nhs_regions_geojson['features'])
region_geojson_df.columns = region_geojson_df.columns.str.lower()
region_geojson_df_1 = region_geojson_df.iloc[:,1:2]
region_geojson_df_1.rename(columns={'attributes.%s' %column_ons_code :'ONS NHS region code'}, inplace=True)
mapped_region_geojson_df = pd.merge(region_geojson_df_1, region_code_map_df, on=['ONS NHS region code', 'ONS NHS region code'], how = 'outer')

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
#Ingest ODS Code table, filtered for CCGs, and generate a difference markdown file from the 

latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, ods_source_path)
ods_codes_table = datalake_download(CONNECTION_STRING, file_system, ods_source_path+latestFolder, ods_source_file)
ods_df = pd.read_parquet(io.BytesIO(ods_codes_table), engine="pyarrow")
ods_df_1 = ods_df[ods_df['ODS_API_Role_Name']=='NHS ENGLAND (REGION)']
ods_df_2= ods_df_1[['Code','Name','Open_Date', 'Close_Date']]
ods_df_2.rename(columns={'Code':'ODS NHS region code', 'Name': 'NHS region name (ODS API Database)', 'Open_Date': 'Open date', 'Close_Date': 'Close date'}, inplace=True)
ods_mappped_df = pd.merge(mapped_region_geojson_df, ods_df_2, on='ODS NHS region code',how='outer')

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
#CCG boundary GeoJSON
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
geojson.dump(nhs_regions_geojson, file_contents, ensure_ascii=False, indent=4)
datalake_upload(file_contents, CONNECTION_STRING, file_system, shapefile_sink_path+current_date_path, shapefile_sink_file)

#CCG ONS to ODS code mapping table
file_contents = io.BytesIO()
mapped_region_geojson_df.to_parquet(file_contents, engine="pyarrow", index=False)
datalake_upload(file_contents, CONNECTION_STRING, file_system, code_maping_sink_path+current_date_path, code_maping_sink_file)

#CCG ODS code mapping to CCG shapefile documentation
file_contents = io.StringIO()
ods_mappped_df.to_markdown(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, markdown_sink_path+current_date_path, markdown_sink_file)


