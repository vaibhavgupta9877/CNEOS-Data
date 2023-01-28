# Databricks notebook source
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import trim
import pandas as pd
import numpy as np
import json
import requests
from datetime import datetime, timedelta
from dateutil import parser

# COMMAND ----------

spark = SparkSession.builder.appName('CNEO_data_extractor').getOrCreate()
spark

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# Azure blob SAS information
# sas_token = '?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupytfx&se=2023-04-13T18:17:11Z&st=2023-01-05T10:17:11Z&spr=https,http&sig=0YnugsQU3rEqee%2Fd3d6TND1H2Gij%2F79EBE0a8gCPi4U%3D'
# sas_url = 'https://myazurefreetier.blob.core.windows.net/cneosdata?sp=racwd&st=2023-01-05T09:00:09Z&se=2023-03-31T17:00:09Z&spr=https&sv=2021-06-08&sr=c&sig=I3V3ujWiA5X%2FRHVwFwFr5%2BbFoUF6AnsihBrO%2FBWUu38%3D'
storage_account = 'myazurefreetier'
container = 'cneosdata'
# azure_file_path = 'https://myazurefreetier.blob.core.windows.net/cneosdata/rawdata.csv'
mount_point = '/mnt/files'

application_id = 'cd56d143-36ca-46e9-a6ed-a34adf4b8582'
auth_key = dbutils.secrets.get(scope='AzureKeyVault', key='storageaccountsecret')
tenet_id = '45007933-edf0-4605-8c19-83ff82416cf8'

endpoint = "https://login.microsoftonline.com/" + tenet_id + "/oauth2/token"

source = "abfss://" + container + "@" + storage_account + ".dfs.core.windows.net/"

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": auth_key,
          "fs.azure.account.oauth2.client.endpoint": endpoint}



# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
      source = source,
      mount_point = mount_point,
      extra_configs = configs)

# COMMAND ----------

# dbutils.fs.mounts()[4].mountPoint

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/files"

# COMMAND ----------

# spark.conf.set("fs.azure.account.auth.type.myazurefreetier.dfs.core.windows.net", "SAS")
# spark.conf.set("fs.azure.sas.token.provider.type.myazurefreetier.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# spark.conf.set("fs.azure.sas.fixed.token.myazurefreetier.dfs.core.windows.net", "sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupytfx&se=2023-04-13T18:17:11Z&st=2023-01-05T10:17:11Z&spr=https,http&sig=0YnugsQU3rEqee%2Fd3d6TND1H2Gij%2F79EBE0a8gCPi4U%3D")


# COMMAND ----------

legacy_data = spark.read.csv('/mnt/files/updatedData.csv', header=True, inferSchema=True)
display(legacy_data)


# COMMAND ----------

last_record = legacy_data.tail(1)
last_record

# COMMAND ----------

last_record_date = last_record[0]['Close-Approach Date']
last_record_date = parser.parse(last_record_date)
last_record_date = last_record_date + timedelta(days=1)
last_record_date = str(last_record_date.date())
last_record_date
# last_record_date = last_record_date.split('-')
# last_record_date = last_record_date[2] + '-' + last_record_date[1] + '-' + last_record_date[0]
# datetime. + timedelta(days=1)

# COMMAND ----------

url = "https://ssd-api.jpl.nasa.gov/cad.api"
parameters = {
    "date-min": last_record_date,
    "date-max": str(datetime.today().date() + timedelta(days=5)),
    "dist-max": "0.05",
    'fullname': "true",
    'dist-max': "0.1",
    'diameter': "true"
}
response = requests.get(url, parameters)
data = response.json()


# COMMAND ----------

data['count']

# COMMAND ----------

columns = [
    'Designation',
    'Orbit Id',
    'Time of Close approach',
    'Close-Approach Date',
    'Nominal Approch distance (au)',
    'Min Close-Approach Distance (au)',
    'Max Close-Approach Distance (au)',
    'V Reletive (Km/s)',
    'V Infinite (Km/s)',
    'Close-Approach Uncertain Time',
    'Absolute Magnitude (mag)',
    'Diameter (Km)',
    'Diameter-Sigma (Km)',
    'Object'
]


# COMMAND ----------

df = pd.DataFrame(data['data'], columns=data['fields'])
df.head()

# COMMAND ----------

df['jd'] = pd.to_numeric(df['jd'])
df['cd'] = pd.to_datetime(df['cd'])
df['dist'] = pd.to_numeric(df['dist'])
df['dist_min'] = pd.to_numeric(df['dist_min'])
df['dist_max'] = pd.to_numeric(df['dist_max'])
df['v_rel'] = pd.to_numeric(df['v_rel'])
df['v_inf'] = pd.to_numeric(df['v_inf'])
df['t_sigma_f'] = df['t_sigma_f'].astype(str)
df['h'] = pd.to_numeric(df['h'])
df['diameter'] = pd.to_numeric(df['diameter'])
df['diameter_sigma'] = pd.to_numeric(df['diameter_sigma'])
df.tail()

# COMMAND ----------

df.columns = columns
df.head(1)

# COMMAND ----------

nsdf = spark.createDataFrame(df)
nsdf

# COMMAND ----------

nsdf = nsdf.fillna(value=-1)
# nsdf.show(1)

# COMMAND ----------

updated_data = legacy_data.union(nsdf)

# COMMAND ----------

print(updated_data.count())
print(legacy_data.count())
print(nsdf.count())


# COMMAND ----------

# updated_data.show(2)

# COMMAND ----------

updated_data = updated_data.fillna(value=-1)

# COMMAND ----------

# updated_data.show(2)

# COMMAND ----------

if data['count'] != "0":
    updated_data.toPandas().to_csv('/dbfs/mnt/files/updatedData.csv', index = False)
else:
    print("There were no new records fetched today")

# COMMAND ----------

