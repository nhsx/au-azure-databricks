# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_analytics_github_gitlab_openrepos_raw.py
DESCRIPTION:
                Databricks notebook with code to ingest new raw data and append to historical
                data for the NHSX Analyticus unit metric: Number of health and care projects shared in open repositories (M027)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        24 Nov. 2021
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
from datetime import timedelta
import json

# 3rd party:
import time
import pandas as pd
import numpy as np
import requests
from pathlib import Path
import urllib.request
from urllib import request as urlreq
from bs4 import BeautifulSoup
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
file_name_config = "config_openrepos_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

# Read parameters from JSON config
# -------------------------------------------------------------------------
file_system = config_JSON['pipeline']['adl_file_system']
sink_path = config_JSON['pipeline']['raw']['sink_path']
sink_file = config_JSON['pipeline']['raw']['sink_file']

# COMMAND ----------

#Ingestion of raw data from the GitHub and GitLab API
# -------------------------------------------------------------------------

github_orgs = [
        "MHRA",
        "publichealthengland",
        "CQCDigital",
        "Health-Education-England",
        "NHS-Blood-and-Transplant",
        "NHSDigital",
        "nhsconnect",
        "111Online",
        "nhsuk",
        "nhsengland",
        "nice-digital",
    ]
df_github = pd.DataFrame()
for org in github_orgs:
  data = [1]
  page = 1
  while bool(data) is True:
    url = (
      "https://api.github.com/orgs/"
      + str(org)
      + "/repos?page="
      + str(page)
      + "&per_page=100"
    )
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    flat_data = pd.json_normalize(data)
    df_github = df_github.append(flat_data)
    page = page + 1
df_github["open_repos"] = 1
df_github = df_github[
        [
            "owner.login",
            "owner.html_url",
            "created_at",
            "open_repos"
        ]
    ].rename(
        columns={
            "owner.login": "org",
            "owner.html_url": "link",
            "created_at": "date"
        }
    )
gitlab_groups = [2955125]
df_gitlab = pd.DataFrame()
for group in gitlab_groups:
  data = [1]
  page = 1
  while bool(data) is True:
    url = (
                "https://gitlab.com/api/v4/groups/"
                + str(group)
                + "/projects?include_subgroups=true&page="
                + str(page)
                + "&per_page=100"
            )
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    flat_data = pd.json_normalize(data)
    df_gitlab = df_gitlab.append(flat_data)
    page = page + 1
    time.sleep(0.2)
df_gitlab["org"] = df_gitlab["namespace.full_path"].apply(lambda x: x.split("/")[0])
df_gitlab["link"] = "https://gitlab.com/" + df_gitlab["org"]
df_gitlab["open_repos"] = 1
df_gitlab = df_gitlab[
        [
            "org",
            "link",
            "created_at",
            "open_repos"
        ]
    ].rename(
        columns={
            "created_at": "date"
        }
    )
df_combined = pd.concat([df_github, df_gitlab]).reset_index(drop=True)
df_combined["date"] = pd.to_datetime(df_combined["date"]).apply(
        lambda x: x.strftime("%Y-%m")
    )
df_combined_sum = (
        df_combined.groupby(["date"])
        .sum()
        .reset_index()
        .rename(columns={"date": "Date", "open_repos": "Number of new open repositories"})
    )
df_combined_sum["Number of new open repositories"] = df_combined_sum[
        "Number of new open repositories"
    ].fillna(0)

# COMMAND ----------

#Full list of dates between first time point and last timepoint
# --------------------------------------------------------------
first_date = df_combined_sum["Date"].min()
last_date = df_combined_sum["Date"].max()
df_date_check = pd.DataFrame()
df_date_check['Date'] = pd.date_range(first_date, last_date, freq='MS').to_period('m').astype(str)
df_date_check_1 = pd.merge(df_date_check, df_combined_sum, on="Date", how='left').fillna(0)

# COMMAND ----------

#Calculate the cummulative number of repositories
# --------------------------------------------------------------

df_combined_cumsum = (
        df_date_check_1.groupby(["Date"])
        .sum()
        .cumsum()
        .reset_index()
        .rename(
            columns={
                "Number of new open repositories": "Cumulative number of open repositories"
            }
        )
    )
df_combined_sum_cumssum = pd.merge(
        df_date_check_1, df_combined_cumsum, on="Date", how="outer"
    )
df_combined_sum_cumssum.index.name = "Unique ID"
df_raw = df_combined_sum_cumssum.copy()

# COMMAND ----------

# Upload raw data snapshot to datalake
current_date_path = datetime.now().strftime('%Y-%m-%d') + '/'
file_contents = io.StringIO()
df_raw.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+current_date_path, sink_file)
