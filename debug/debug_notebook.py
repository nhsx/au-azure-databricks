# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           debug.py
DESCRIPTION:
                debug databricks notebook
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        18 Aug 2021
VERSION:        0.0.2
"""


# COMMAND ----------

# Install libs
# -----------------------------------------------------
%pip install pandas pathlib azure-storage-file-datalake

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
from pathlib import Path
#import libify
from azure.storage.filedatalake import DataLakeServiceClient

# Internal:
# %run /Repos/nhsx-au-analytics/au-azure-databricks/functions/datalake_instantiate_client
# mod1 = libify.importer(globals(), '/path/to/importee1')
# mod2 = libify.importer(globals(), '/path/to/importee2')

# Connect to Azure datalake
# -------------------------------------------------------------------------
# !env from databricks secrets
CONNECTION_STRING = dbutils.secrets.get(scope="datalakefs", key="CONNECTION_STRING")


# COMMAND ----------

# Helper functions
# -------------------------------------------------------------------------
def datalake_download(CONNECTION_STRING, FILE_SYSTEM, FILE_PATH, FILE_NAME):
    r"""Returns pandas dataframe from csv file on azure data lake.

    Parameters
    ----------
    CONNECTION_STRING : str
        !env variable from func app settings
    FILE_SYSTEM : str
        azure datalake file system name.
    FILE_PATH : str
        path to file (sans latest folder) on azure datalake.
    FILE_NAME : str
        csv file name on azure datalake.

    Returns
    -------
    dataframe : pandas.core.frame.DataFrame
        pandas dataframe of csv file on azure datalake.
    """
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(file_system=FILE_SYSTEM)
    directory_client = file_system_client.get_directory_client(FILE_PATH)
    file_client = directory_client.get_file_client(FILE_NAME)
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    return downloaded_bytes
  
def datalake_upload(file, CONNECTION_STRING, FILE_SYSTEM, FILE_PATH, FILE_NAME):
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(file_system=FILE_SYSTEM)
    directory_client = file_system_client.get_directory_client(FILE_PATH)
    file_client = directory_client.create_file(FILE_NAME)
    file_length = file_contents.tell()
    file_client.upload_data(file_contents.getvalue(), length=file_length, overwrite=True)
    return '200 OK'

def datalake_latestFolder(CONNECTION_STRING, FILE_SYSTEM, FILE_PATH):
    try:
        service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
        file_system_client = service_client.get_file_system_client(
            file_system=FILE_SYSTEM
        )
        pathlist = list(file_system_client.get_paths(FILE_PATH))
        folders = []
        # remove FILE_PATH and file names from list
        for path in pathlist:
            folders.append(
                path.name.replace(FILE_PATH.strip("/"), "")
                .lstrip("/")
                .rsplit("/", 1)[0]
            )
        folders.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"), reverse=True)
        latestFolder = folders[0]+"/"
        return latestFolder
    except Exception as e:
        print(e)

# COMMAND ----------

# Download JSON config from Azure datalake
# -------------------------------------------------------------------------
FILE_PATH = "/config/pipelines/debug/"
FILE_NAME = "config_debug.json"
FILE_SYSTEM = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, FILE_SYSTEM, FILE_PATH, FILE_NAME)
config_JSON = json.loads(io.BytesIO(config_JSON).read())


# COMMAND ----------

source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
sink_path = config_JSON['pipeline']['project']['sink_path']
sink_file = config_JSON['pipeline']['project']['sink_file']
file_system = config_JSON['pipeline']['adl_file_system']

# COMMAND ----------

#latestFolder test
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
latestFolder

# COMMAND ----------

source_path+latestFolder+source_file

# COMMAND ----------

# Download test
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")
df.head()

# COMMAND ----------

# Upload test
file_contents = io.BytesIO()
df.to_parquet(file_contents, engine="pyarrow", index=False)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
