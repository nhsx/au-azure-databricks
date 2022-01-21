# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_cybersecurity_dspt_gp_practices_standards_nosubmission_year_count_prop.py
DESCRIPTION:
                Databricks notebook with processing code for the NHSX Analyticus unit metric: M078A: No. and % of GP practices that have not submitted 20/21 DSPT assessment (yearly historical)
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli, Muhammad-Faaiz Shanawas
CONTACT:        data@nhsx.nhs.uk
CREATED:        20 Jan. 2021
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
file_name_config = "config_dspt_gp_practices_historical_dbrks.json"
file_system_config = "nhsxdatalakesagen2fsprod"
config_JSON = datalake_download(CONNECTION_STRING, file_system_config, file_path_config, file_name_config)
config_JSON = json.loads(io.BytesIO(config_JSON).read())

# COMMAND ----------

#Get parameters from JSON config
file_system = config_JSON['pipeline']['adl_file_system']
source_path = config_JSON['pipeline']['project']['source_path']
source_file = config_JSON['pipeline']['project']['source_file']
sink_path = config_JSON['pipeline']['project']['databricks'][2]['sink_path']
sink_file = config_JSON['pipeline']['project']['databricks'][2]['sink_file']

# COMMAND ----------

# Processing
# -------------------------------------------------------------------------
latestFolder = datalake_latestFolder(CONNECTION_STRING, file_system, source_path)
file = datalake_download(CONNECTION_STRING, file_system, source_path+latestFolder, source_file)
df = pd.read_parquet(io.BytesIO(file), engine="pyarrow")

# COMMAND ----------

#this function is ued in set_flag_conditions to check if the current DSPT status contains years in their labels by checking for digits in the status
def contains_digits(input_string):
  flag = False
  for character in input_string:
    if character.isdigit() == True:
      flag = True
  return flag #returns a true value if the status has got digits and false otherwise

def set_flags_m78a(df, year):
  #initialise m78a column
  num_rows = df.shape[0]
  m78a_init_list = [0] * num_rows
  
  #we will name this M076 for initialisation to make it easier to set the flags
  #this will be renamed in the format dataframe function
  df["M078"] = m78a_init_list

  #first we define the current year being processed and previous year as e.g 18 and 19 for 2018 and 2019
  #we also define the current and next year as some exceptions publish according to next year standards 
  last_year = year - 1
  next_year = year + 1
  year_shortened = str(year)[2:4]
  last_year_shortened = str(last_year)[2:4]
  next_year_shortened = str(next_year)[2:4]
  

  #we iterate through the rows in the dataframe under the Status_Raw column using the indexes
  for index in range(0, num_rows):
    current_status = str(df.Status_Raw[index])
  #we have 2 sets of conditions, 1 if the status contains digits/years and 1 otherwise
    if (contains_digits(current_status) == True):
      #check if the status (contains the current and last year e.g 18/19 for 2019) or the (current year and the next year e.g 19/20 for 2019) to meet or exceed DSPT requirements as specified
      if ((year_shortened in current_status) and (last_year_shortened in current_status) or ((year_shortened in current_status) and (next_year_shortened in current_status))):
        if (not(("MET" in current_status.upper()) or ("EXCEEDED" in current_status.upper()))):
          df.M078[index] = 1

    else:
      #similar conditions for if the status does not contain the year we just need to check for the word only
      if ("PUBLISHED" in current_status.upper())
        df.M078[index] = 1
      #finally check if the status is blank or null then we count this as not published and set M078 flag to 1 and others to 0
      if pd.isnull(df['Status_Raw'].iloc[index]):
        df.M078[index] = 1
      
  return df
  
  
#run this function to format the dataframe after setting flags for m76a
def format_dspt_dataframe(df):
  #rename index column as Unique ID
  df["Unique ID"] = df.index.name
  #rename code as Practice code
  df.rename(columns={"Code":"Practice code"}, inplace = True)
  #rename DSPT_edition as Date
  df.rename(columns={"DSPT_Edition":"Date"}, inplace = True)
  #rename M078 column
  df.rename(columns={"M078":"Number of GP Practices That Did Not Meet or Publish the DSPT Standard"})
  #drop snapshot_date and Status_Raw
  df = df.drop(["Snapshot_Date", "Status_Raw"], axis = 1)

# COMMAND ----------

#code to run functions which will set flags and format the dataframe

#get this function from the helper functions to be used in set_flags
#if this is not possible can recode this function
year = get_year_dspt_gp(upload_date)

df = set_flags_m78a(df, year)
df = format_dspt_dataframe(df)

# COMMAND ----------

#Upload processed data to datalake
file_contents = io.StringIO()
df_processed.to_csv(file_contents)
datalake_upload(file_contents, CONNECTION_STRING, file_system, sink_path+latestFolder, sink_file)
