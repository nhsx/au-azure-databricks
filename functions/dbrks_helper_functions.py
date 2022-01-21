# Databricks notebook source
#!/usr/bin python3

# -------------------------------------------------------------------------
# Copyright (c) 2021 NHS England and NHS Improvement. All rights reserved.
# Licensed under the MIT License. See license.txt in the project root for
# license information.
# -------------------------------------------------------------------------

"""
FILE:           dbrks_helper_functions.py
DESCRIPTION:
                Helper functons needed for the databricks processing step of ETL pipelines
USAGE:
                ...
CONTRIBUTORS:   Craig Shenton, Mattia Ficarelli
CONTACT:        data@nhsx.nhs.uk
CREATED:        01 Sept 2021
VERSION:        0.0.1
"""


# COMMAND ----------

# Helper functions
# -------------------------------------------------------------------------
def datalake_download(CONNECTION_STRING, file_system, source_path, source_file):
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    directory_client = file_system_client.get_directory_client(source_path)
    file_client = directory_client.get_file_client(source_file)
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    return downloaded_bytes
  
def datalake_upload(file, CONNECTION_STRING, file_system, sink_path, sink_file):
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    directory_client = file_system_client.get_directory_client(sink_path)
    file_client = directory_client.create_file(sink_file)
    file_length = file_contents.tell()
    file_client.upload_data(file_contents.getvalue(), length=file_length, overwrite=True)
    return '200 OK'
  
def datalake_latestFolder(CONNECTION_STRING, file_system, source_path):
  try:
      service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
      file_system_client = service_client.get_file_system_client(file_system=file_system)
      pathlist = list(file_system_client.get_paths(source_path))
      folders = []
      # remove file_path and source_file from list
      for path in pathlist:
        folders.append(path.name.replace(source_path.strip("/"), "").lstrip("/").rsplit("/", 1)[0])
        folders.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"), reverse=True)
      latestFolder = folders[0]+"/"
      return latestFolder
  except Exception as e:
      print(e)

# COMMAND ----------

# Ingestion and analytical functions
# -------------------------------------------------------------------------
def ons_geoportal_file_download(search_url, url_start, string_filter):
  url_2 = '/0/query?where=1%3D1&outFields=*&outSR=4326&f=json'
  page = requests.get(search_url)
  response = urlreq.urlopen(search_url)
  soup = BeautifulSoup(response.read(), "lxml")
  data_url = soup.find_all('a', href=re.compile(string_filter))[-1].get('href')
  full_url = url_start + data_url + url_2
  with urlopen(full_url)  as response:
      json_file = json.load(response)
  return json_file

def datalake_listContents(CONNECTION_STRING, file_system, source_path):
  try:
    service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)
    file_system_client = service_client.get_file_system_client(file_system=file_system)
    folder_path = file_system_client.get_paths(path=source_path)
    file_list = []
    for path in folder_path:
      file_list.append(path.name.replace(source_path.strip("/"), "").lstrip("/").rsplit("/", 1)[0])
    return file_list
  except Exception as e:
    print(e)
    
#Renaming columns for the DSPT GP dataframe
def rename_dspt_gp_cols(df):
  column_list = []
  column_name = ''
  for cols in df.columns:
    column_list.append(cols)
  for col in column_list:
    if "PRIMARY" in col.upper(): #.upper() is used in case some headings are lower case or upper case
      df.rename(columns={col:"Primary Sector"}, inplace = True)
    elif "CODE" in col.upper():
      df.rename(columns={col:"Code"}, inplace = True)
    elif "ORGANISATION" in col.upper():
      df.rename(columns={col:"Organisation_Name"}, inplace = True)
    elif "STATUS" in col.upper():
      df.rename(columns={col:"Status_Raw"}, inplace = True)
  return df #returns the renamed dataframe

def process_dspt_dataframe(df, filename, year):
  df = rename_dspt_gp_cols(df) #call the rename_cols function on the dataframe to syncronise all the column names
  df_gp = df[df["Primary Sector"] == "GP"] #filter the primary sector column for GP's only 
  num_rows = df_gp.shape[0] #retrieve the number of rows after filtering to be create the right size columns(lists) for the dspt_edition and snapshot_date
  dspt_edition = get_dspt_edition(year, num_rows) #call and save the columns(list) for dspt_edition and snapshot_date
  snapshot_date = get_snapshot_date(year, filename, num_rows)
  df_gp = df_gp.assign(DSPT_Edition = dspt_edition) #assign these columns to the dataframe
  df_gp = df_gp.assign(Snapshot_Date = snapshot_date)
  df_gp = df_gp[['Code', 'Organisation_Name', 'DSPT_Edition', 'Snapshot_Date', 'Status_Raw']] #drop all the columns we do not need for the final curation and keep all relevant ones as specified below
  df_gp = df_gp.reset_index(drop = True) #reset the indexes as these will all be different after filtering
  return df_gp

#fetching data from the filename - get dspt edition and snapshot date for appending to new dataframe
def get_year_dspt_gp(upload_date):
  year = int(upload_date[0:4])
  return year

def get_dspt_edition(year, num_rows):
  dspt_edition = str(year - 1) + "/" + str(year) #specifies format as previous_year/current_year
  dspt_edition_list = [dspt_edition] * num_rows #creates the column of size num_rows to be added to dataframe
  return dspt_edition_list 

def get_snapshot_date(year, filename, num_rows):
  filename_sep = filename.split()
  index = 0
  for word in filename_sep:
    if str(year) in word:
      index = filename_sep.index(word)
  date = filename_sep[index]
  date = date.replace("_", "/")  
  snapshot_date_list = [date] * num_rows
  return snapshot_date_list

#this function is ued in set_flag_conditions to check if the current DSPT status contains years in their labels by checking for digits in the status
def contains_digits(input_string):
  flag = False
  for character in input_string:
    if character.isdigit() == True:
      flag = True
  return flag #returns a true value if the status has got digits and false otherwise
