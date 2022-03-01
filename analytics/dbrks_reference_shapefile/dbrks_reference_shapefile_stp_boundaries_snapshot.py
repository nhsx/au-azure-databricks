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
                Databricks notebook with code to process a GeoJSON file from the ONS Geo Portal for STP boundaries into tablular format
                for NHSX Analytics Unit dashboard projects
USAGE:         
CONTRIBUTORS:   Mattia Ficarelli and Craig Shenton
CONTACT:        data@nhsx.nhs.uk
CREATED:        21 Oct. 2021
VERSION:        0.0.1
"""

# COMMAND ----------

# Install libs
# -------------------------------------------------------------------------
%pip install geojson==2.5.* tabulate requests pandas pathlib azure-storage-file-datalake beautifulsoup4 numpy urllib3 lxml regex pyarrow==5.0.* geopandas shapely geopandas shapely 

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
import geopandas as gpd
from shapely.geometry import Point, Polygon, shape
from shapely import wkb, wkt
import shapely.speedups
shapely.speedups.enable()
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
shapefile_source_path = config_JSON['pipeline']['project'][1]['shapefile_source_path']
shapefile_source_file = config_JSON['pipeline']['project'][1]['shapefile_source_file']
shapefile_sink_path = config_JSON['pipeline']['project'][1]['shapefile_sink_path']
shapefile_sink_file = config_JSON['pipeline']['project'][1]['shapefile_sink_file']

# COMMAND ----------

#Processing
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, shapefile_source_path)
file = datalake_download(CONNECTION_STRING, file_system, shapefile_source_path+latestFolder, shapefile_source_file)
df = gpd.read_file(io.BytesIO(file))
column_mapping = {df.columns[0]: 'Unique ID', df.columns[1]: 'STP code', df.columns[2]: 'STP name'}
df_1 = df.rename(columns=column_mapping)
df_2 = df_1.set_index('Unique ID')
df_processed = df_2.copy()

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, shapefile_sink_path+latestFolder, shapefile_sink_file)
