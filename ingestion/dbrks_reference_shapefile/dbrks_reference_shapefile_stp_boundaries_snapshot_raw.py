# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_reference_shapefile_stp_boundaries_snapshot_raw.py
DESCRIPTION:
                Databricks notebook with code to ingest a GeoJSON file with NHS region boundaries from the ONS Geo Portal
                for NHSX Analytics Unit dashboard projects
USAGE:         
CONTRIBUTORS:   Mattia Ficarelli and Craig Shenton
CONTACT:        data@nhsx.nhs.uk
CREATED:        16 Sept 2021
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
shapefile_sink_path = config_JSON['pipeline']['raw']['databricks'][0]["shapefile_sink_path"]
shapefile_sink_file = config_JSON['pipeline']['raw']['databricks'][0]["shapefile_sink_file"]
code_maping_sink_path = config_JSON['pipeline']['raw']['databricks'][0]["code_maping_sink_path"]
code_maping_sink_file = config_JSON['pipeline']['raw']['databricks'][0]["code_maping_sink_file"]

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
#Ingest STP boundary GeoJSON

search_url = "https://ons-inspire.esriuk.com/arcgis/rest/services/Health_Boundaries/"
url_1 = "https://ons-inspire.esriuk.com"
url_2 = '/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'

page = requests.get(search_url)
response = urlreq.urlopen(search_url)
soup = BeautifulSoup(response.read(), "lxml")
stp_url = soup.find_all('a', href=re.compile("Sustainability_and_Transformation_Partnerships"))[-1].get('href')
full_url = url_1 + stp_url  + url_2

with urlopen(full_url)  as response:
    stp_geojson = json.load(response)


# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
#Ingest STP ONS to ODS code mapping table, and map to CCG dataframe generated from the STP boundary GeoJSON

column_ons_code = stp_geojson['fields'][1]['name'].lower()
column_stp_name = stp_geojson['fields'][2]['name'].lower()

search_url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
url_1 = "https://services1.arcgis.com"
url_2 = '/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
page = requests.get(search_url)
response = urlreq.urlopen(search_url)
soup = BeautifulSoup(response.read(), "lxml")
stp_code_map_url = soup.find_all('a', href=re.compile("STP_APR"))[-1].get('href')
full_url = url_1 + stp_code_map_url + url_2
with urlopen(full_url) as response:
    stp_code_map_json = json.load(response)
    stp_code_map_df = pd.json_normalize(stp_code_map_json['features'])

column_ods_code = stp_code_map_json['fields'][1]['name'].lower()
column_ons_code_1 = stp_code_map_json['fields'][0]['name'].lower()
stp_code_map_df = stp_code_map_df.iloc[:,:2]
stp_code_map_df.columns = stp_code_map_df.columns.str.lower()
stp_code_map_df.rename(columns={'attributes.%s' %column_ons_code_1 :'ONS STP code', 'attributes.%s' %column_ods_code: 'ODS STP code'}, inplace=True)
stp_geojson_df = pd.json_normalize(stp_geojson['features'])
stp_geojson_df.columns = stp_geojson_df.columns.str.lower()
stp_geojson_df_1 = stp_geojson_df.iloc[:,1:3]
stp_geojson_df_1.rename(columns={'attributes.%s' %column_ons_code :'ONS STP code', 'attributes.%s' %column_stp_name: 'STP name'}, inplace=True)
mapped_stp_geojson_df = pd.merge(stp_geojson_df_1, stp_code_map_df, on=['ONS STP code', 'ONS STP code'], how = 'outer')

# COMMAND ----------

# Upload processed data to datalake
# -------------------------------------------------------------------------
#STP boundary GeoJSON
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
geojson.dump(stp_geojson, file_contents, ensure_ascii=False, indent=4)
datalake_upload(file_contents, CONNECTION_STRING, file_system, shapefile_sink_path+current_date_path, shapefile_sink_file)

#STP ONS to ODS code mapping table
file_contents = io.BytesIO()
mapped_stp_geojson_df.to_parquet(file_contents, engine="pyarrow", index=False)
datalake_upload(file_contents, CONNECTION_STRING, file_system, code_maping_sink_path+current_date_path, code_maping_sink_file)
