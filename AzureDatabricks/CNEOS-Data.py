# Databricks notebook source
# MAGIC %md
# MAGIC ### CNEOS Data Extractor
# MAGIC #### Batch Data Processing
# MAGIC #### Authored By -- Vaibhav Gupta

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing Modules

# COMMAND ----------

import pandas as pd
import numpy as np
import requests
import json
from pyspark.sql import SparkSession, Row
import pyspark.pandas as ps
from pyspark.sql.functions import udf, col, explode, lit, split, concat, to_timestamp, to_date, date_format, round, trim
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, ArrayType, DateType, TimestampType
from datetime import datetime, timedelta

# COMMAND ----------

# # STEP 1: RUN THIS CELL TO INSTALL BAMBOOLIB

# # You can also install bamboolib on the cluster. Just talk to your cluster admin for that
# %pip install bamboolib  

# # Heads up: this will restart your python kernel, so you may need to re-execute some of your other code cells.

# COMMAND ----------

# # STEP 2: RUN THIS CELL TO IMPORT AND USE BAMBOOLIB

# import bamboolib as bam

# # This opens a UI from which you can import your data
# bam  

# # Already have a pandas data frame? Just display it!
# # Here's an example
# # import pandas as pd
# # df_test = pd.DataFrame(dict(a=[1,2]))
# # df_test  # <- You will see a green button above the data set if you display it

# COMMAND ----------

spark = SparkSession.builder.master('local[*]').appName('CNEOS_Data_Extractor').getOrCreate()
spark

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mounting Azure Blob Storage/ADLS 2

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# Way to mount basic Blob Storage

# Azure blob SAS information
# sas_token = '?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupytfx&se=2023-04-13T18:17:11Z&st=2023-01-05T10:17:11Z&spr=https,http&sig=0YnugsQU3rEqee%2Fd3d6TND1H2Gij%2F79EBE0a8gCPi4U%3D'
# sas_url = 'https://myazurefreetier.blob.core.windows.net/cneosdata?sp=racwd&st=2023-01-05T09:00:09Z&se=2023-03-31T17:00:09Z&spr=https&sv=2021-06-08&sr=c&sig=I3V3ujWiA5X%2FRHVwFwFr5%2BbFoUF6AnsihBrO%2FBWUu38%3D'
storage_account = 'myazurefreetier'
container = 'cneosproject'
# azure_file_path = 'https://myazurefreetier.blob.core.windows.net/cneosdata/rawdata.csv'
mount_point = '/mnt/files'

application_id = dbutils.secrets.get(scope='AzureKeyVault', key='storageAccountSecretAppId')
auth_key = dbutils.secrets.get(scope='AzureKeyVault', key='storageaccountsecret')
tenet_id = dbutils.secrets.get(scope='AzureKeyVault', key='storageAccountSecretTenetId')

endpoint = "https://login.microsoftonline.com/" + tenet_id + "/oauth2/token"

source = "abfss://" + container + "@" + storage_account + ".dfs.core.windows.net/"

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": auth_key,
          "fs.azure.account.oauth2.client.endpoint": endpoint}



# COMMAND ----------

# dbutils.fs.unmount(mount_point = mount_point)

# COMMAND ----------

# Way To connect ADLS Gen 2

# Optionally, you can add <directory-name> to the source URI of your mount point.
if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    dbutils.fs.mount(
      source = source,
      mount_point = mount_point,
      extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/files/"

# COMMAND ----------

# dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Connecing To Azure Data Lake Store Gen 2

# COMMAND ----------

# application_id = dbutils.secrets.get(scope='AzureKeyVault', key='storageAccountSecretAppId')
# auth_key = dbutils.secrets.get(scope='AzureKeyVault', key='storageaccountsecret')
# tenet_id = dbutils.secrets.get(scope='AzureKeyVault', key='storageAccountSecretTenetId')


# COMMAND ----------

# service_credential = dbutils.secrets.get(scope="AzureKeyVault",key="storageaccountsecret")

# spark.conf.set("fs.azure.account.auth.type.myazurefreetier.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.myazurefreetier.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.myazurefreetier.dfs.core.windows.net", application_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.myazurefreetier.dfs.core.windows.net", auth_key)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.myazurefreetier.dfs.core.windows.net", "https://login.microsoftonline.com/" + tenet_id + "/oauth2/token")


# COMMAND ----------

# storage_account = 'myazurefreetier'
# container = 'cneosproject'

# source = "abfss://" + container + "@" + storage_account + ".dfs.core.windows.net/"

# dbutils.fs.ls(source)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Using ADLFS Package

# COMMAND ----------

# storage_options={'account_name': storage_account, 'tenant_id': tenet_id, 'client_id': application_id, 'client_secret': auth_key}
# adls2_path = 'abfs://' + container + '/rawInput'
# dbutils.fd.ls(adls2_path, storage_options=storage_options)

# COMMAND ----------

date = datetime.today().date()
date_min = date + timedelta(days=59)
date_max = date - timedelta(days=60)
print(date_min, date_max)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Triggering Get API Request

# COMMAND ----------

def makeAPICall(url, parameters):
    try:
        response = requests.get(url, params=parameters)
    except Exception as e:
        return e

    if response.status_code == 200 and response != None:
        return response.json()
    return None

# COMMAND ----------

url = "https://ssd-api.jpl.nasa.gov/cad.api"
parameters = {
#     "date-min": str(datetime.today().date()),
    "date-min": "1900-01-04",
    "date-max": str(date_max),
    "dist-max": "2.5",
    'fullname': "true",
    # 'dist-max': "0.1",
    'diameter': "true"
}
# response = requests.get(url, parameters)
# data = response.json()

# response = makeAPICall(url, parameters)

# COMMAND ----------

response = makeAPICall(url, parameters)

# COMMAND ----------

response['count']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stating Data Attributes

# COMMAND ----------

# schema = StructType([
#     StructField("Signature", StringType(), True),
#     StructField("Count", StringType(), True),
#     StructField("Fields", ArrayType(
#         StructType([
#             StructField("Designation", StringType(), True),
#             StructField("Orbit_Id", StringType(), True),
#             StructField("Time of Close approach", StringType(), True),
#             StructField("Close-Approach Date", StringType(), True),
#             StructField("Nominal Approch distance (au)", StringType(), True),
#             StructField("Min Close-Approach Distance (au)",
#                         StringType(), True),
#             StructField("Max Close-Approach Distance (au)",
#                         StringType(), True),
#             StructField("V Reletive (Km/s)", StringType(), True),
#             StructField("V Infinite (Km/s)", StringType(), True),
#             StructField("Close-Approach Uncertain Time", StringType(), True),
#             StructField("Absolute Magnitude (mag)", StringType(), True),
#             StructField("Diameter (Km)", StringType(), True),
#             StructField("Diameter-Sigma (Km)", StringType(), True),
#             StructField("Designation", StringType(), True),
#         ])
#     ), True),
#     StructField("Data", ArrayType(
#         StructType([
#             StructField("Designation", StringType(), True),
#             StructField("Orbit_Id", StringType(), True),
#             StructField("Time of Close approach", DoubleType(), True),
#             StructField("Close-Approach Date", DateType(), True),
#             StructField("Nominal Approch distance (au)", DoubleType(), True),
#             StructField("Min Close-Approach Distance (au)",
#                         DoubleType(), True),
#             StructField("Max Close-Approach Distance (au)",
#                         DoubleType(), True),
#             StructField("V Reletive (Km/s)", DoubleType(), True),
#             StructField("V Infinite (Km/s)", DoubleType(), True),
#             StructField("Close-Approach Uncertain Time", StringType(), True),
#             StructField("Absolute Magnitude (mag)", DoubleType(), True),
#             StructField("Diameter (Km)", DoubleType(), True),
#             StructField("Diameter-Sigma (Km)", DoubleType(), True),
#             StructField("Designation", StringType(), True),
#         ])
#     ), True)
# ])
# schema


# COMMAND ----------

columns = [
    'Designation',
    'Orbit_Id',
    'Time_of_Close_approach',
    'Close_Approach_Date',
    'Nominal_Approch_distance_au',
    'Min_Close_Approach_Distance_au',
    'Max_Close_Approach_Distance_au',
    'V_Reletive_Kms',
    'V_Infinite_Kms',
    'Close_Approach_Uncertain_Time',
    'Absolute_Magnitude_mag',
    'Diameter_Km',
    'Diameter_Sigma_Km',
    'Object'
]


# COMMAND ----------

# def formattingData(data):
#     formattedData = []
#     for row in data:
#         temp = {}
#         for j in range(len(row)):
#             temp[columns[j]] = row[j]
#         formattedData.append(temp)
#     return formattedData


# COMMAND ----------

# formattingData(response['data'])

# COMMAND ----------

# data = formattingData(response['data'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Data to Spark Pandas Dataframe

# COMMAND ----------

df = ps.DataFrame(response['data'], columns=columns)
df.head()

# COMMAND ----------

df.tail()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Checking stats of raw API Data

# COMMAND ----------

df.shape

# COMMAND ----------

# df.describe()

# COMMAND ----------

# df[columns[0]] = df[columns[0]].astype(str)
df.dtypes

# COMMAND ----------

df.tail()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Spark Pandas DataFrame to Spark SQL DataFrame

# COMMAND ----------

sdf = df.to_spark()


# COMMAND ----------

# pdf = sdf.toPandas()
rawInputPath = '/dbfs/mnt/files/rawInput/rawInput.parquet'
rawInputPath
sdf.toPandas().to_parquet(rawInputPath, index = False)
# # sdf.coalesce(1).write.mode('overwrite').csv(source + '/rawInput/rawInput.csv', mode='append', header=True)

# COMMAND ----------

sdf.printSchema()

# COMMAND ----------

sdf.show(4)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Performing Some DataType Conversions

# COMMAND ----------

sdf2 = sdf.withColumn("Designation", col("Designation").cast(StringType())) \
    .withColumn("Orbit_Id", col("Orbit_Id").cast(StringType())) \
    .withColumn("Time_of_Close_approach", col("Time_of_Close_approach").cast(DoubleType())) \
    .withColumn("Close_Approach_Date", col("Close_Approach_Date").cast(StringType())) \
    .withColumn("Nominal_Approch_distance_au", col("Nominal_Approch_distance_au").cast(DoubleType())) \
    .withColumn("Min_Close_Approach_Distance_au", col("Min_Close_Approach_Distance_au").cast(DoubleType())) \
    .withColumn("Max_Close_Approach_Distance_au", col("Max_Close_Approach_Distance_au").cast(DoubleType())) \
    .withColumn("V_Reletive_Kms", col("V_Reletive_Kms").cast(DoubleType())) \
    .withColumn("V_Infinite_Kms", col("V_Infinite_Kms").cast(DoubleType())) \
    .withColumn("Close_Approach_Uncertain_Time", col("Close_Approach_Uncertain_Time").cast(StringType())) \
    .withColumn("Absolute_Magnitude_mag", col("Absolute_Magnitude_mag").cast(DoubleType())) \
    .withColumn("Diameter_Km", col("Diameter_Km").cast(DoubleType())) \
    .withColumn("Diameter_Sigma_Km", col("Diameter_Sigma_Km").cast(DoubleType())) \
    .withColumn("Object", col("Object").cast(StringType()))
sdf2.printSchema()

# COMMAND ----------

sdf2.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transforming Data Based On Following Conditions
# MAGIC 
# MAGIC - ###### Object
# MAGIC 
# MAGIC     Object primary designation
# MAGIC - ###### Close-Approach (Close Approach) Date
# MAGIC 
# MAGIC     Date and time (TDB) of closest Earth approach. "Nominal Date" is given to appropriate precision. The 3-sigma uncertainty in the time is given in the +/- column in days_hours:minutes format (for example, "2_15:23" is 2 days, 15 hours, 23 minutes; "< 00:01" is less than 1 minute).
# MAGIC     
# MAGIC - ###### Close Approach Distance Nominal (au)/(km)
# MAGIC 
# MAGIC     The most likely (Nominal) close-approach distance (Earth center to NEO center), in astronomical units.
# MAGIC - ###### Close Approach Distance Minimum (au)/(km)
# MAGIC 
# MAGIC     The minimum possible close-approach distance (Earth center to NEO center), in astronomical units. The minimum possible distance is based on the 3-sigma Earth target-plane error ellipse.
# MAGIC - ###### Close Approach Distance Maximum (au)/(km)
# MAGIC 
# MAGIC     The maximum possible close-approach distance (Earth center to NEO center), in astronomical units. The maximum possible distance is based on the 3-sigma Earth target-plane error ellipse.
# MAGIC - ###### V relative (km/s)
# MAGIC 
# MAGIC     Object velocity relative to Earth at close-approach.
# MAGIC - ###### V infinity (km/s)
# MAGIC 
# MAGIC     Object velocity relative to a massless Earth at close-approach.
# MAGIC - ###### Absolute Magnitute H (mag)
# MAGIC 
# MAGIC     Asteroid absolute magnitude (in general, smaller H implies larger asteroid diameter). Undefined for comets.
# MAGIC - ###### Diameter (km)
# MAGIC 
# MAGIC     Diameter value when known or a range (min - max) estimated using the asteroid's absolute magnitude (H) and limiting albedos of 0.25 and 0.05.
# MAGIC - ###### au
# MAGIC 
# MAGIC     One Astronomical Unit (au) is approximately 150 million kilometers (see glossary for definition).
# MAGIC - ###### LD
# MAGIC 
# MAGIC     One Lunar Distance (LD) is approximately 384,000 kilometers (see glossary for definition).

# COMMAND ----------

sdf2 = sdf2.withColumn('Nominal_Approch_distance_km', lit(round(col('Nominal_Approch_distance_au')*149597870.7, 0))) \
        .withColumn('Time_of_Close_approach', lit(round(col('Time_of_Close_approach'), 2))) \
        .withColumn('Nominal_Approch_distance_au', lit(round(col('Nominal_Approch_distance_au'), 5))) \
        .withColumn('Min_Close_Approach_Distance_au', lit(round(col('Min_Close_Approach_Distance_au'), 5))) \
        .withColumn('Max_Close_Approach_Distance_au', lit(round(col('Max_Close_Approach_Distance_au'), 5))) \
        .withColumn('Min_Close_Approach_Distance_km', lit(round(col('Min_Close_Approach_Distance_au')*149597870.7, 0))) \
        .withColumn('Max_Close_Approach_Distance_km', lit(round(col('Max_Close_Approach_Distance_au')*149597870.7, 0))) \
        .withColumn('V_Reletive_Kms', lit(round(col('V_Reletive_Kms'), 2))) \
        .withColumn('V_Infinite_Kms', lit(round(col('V_Infinite_Kms'), 2))) \
        .withColumn('Designation', trim(col('Designation'))) \
        .withColumn('Object', trim(col('Object'))) \
        .withColumn('Diameter_Km', concat(lit(round(((1329 * (10 ** ((-0.2) * col('Absolute_Magnitude_mag'))))/0.25 ** 0.5), 3)), \
                                          lit(' - '), \
                                          lit(round(((1329 * (10 ** ((-0.2) * col('Absolute_Magnitude_mag'))))/0.05 ** 0.5), 3)))) \
        .withColumn('Close_Approach_Date_formatted', concat(col('Close_Approach_Date'), lit(':00:000 ± '), lit(col('Close_Approach_Uncertain_Time'))))


# COMMAND ----------

display(sdf2)

# COMMAND ----------

del sdf

# COMMAND ----------

sdf = sdf2.alias('sdf')


# COMMAND ----------

sdf.printSchema()

# COMMAND ----------

# sdf = sdf.drop('Designation', 'Time_of_Close_approach','Close_Approach_Date', 'Close_Approach_Uncertain_Time', 'Diameter_Sigma_Km')
# sdf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Finalizing Formatted and Transformed Data

# COMMAND ----------

final_sdf = sdf.select('Designation', 'Close_Approach_Date_formatted', 'Orbit_Id', 'Nominal_Approch_distance_au', 'Nominal_Approch_distance_km', 'Min_Close_Approach_Distance_au', 'Min_Close_Approach_Distance_km', 'Max_Close_Approach_Distance_au', 'Max_Close_Approach_Distance_km', 'V_Reletive_Kms', 'V_Infinite_Kms', 'Absolute_Magnitude_mag', 'Diameter_Km')

# COMMAND ----------

display(sdf)

# COMMAND ----------

display(final_sdf)

# COMMAND ----------

# rawInputPath
processedDataPath = '/dbfs/mnt/files/processedData/processedData.parquet'
final_sdf.toPandas().to_parquet(processedDataPath, index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Connecting to Azure SQL Database
# MAGIC  - Creating Table CNEOS_Data
# MAGIC  
# MAGIC  -- Refer link https://medium.com/codex/get-started-with-azure-sql-in-databricks-9bfa8d590c64

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Reading Processed File from ADLS 2

# COMMAND ----------

processedDataPath_read = '/mnt/files/processedData/processedData.parquet'
final_df = spark.read.parquet(processedDataPath_read)
final_df.printSchema()

# COMMAND ----------

display(final_df)

# COMMAND ----------

sql_user = dbutils.secrets.get(scope='AzureKeyVault', key='freecneosdbuser')
sql_password = dbutils.secrets.get(scope='AzureKeyVault', key='freecneosdbpassword')
sql_user_synapse = dbutils.secrets.get(scope='AzureKeyVault', key='freesynapsecneospooluser')
sql_password_synapse = dbutils.secrets.get(scope='AzureKeyVault', key='freesynapsecneospoolpassword')

# url = "jdbc:sqlserver://mydataengineering.database.windows.net:1433;database=cneos;user=" + sql_user + "@mydataengineering;password=" + sql_password + ";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

jdbcHostname = "mydataengineering.database.windows.net"
jdbcDatabase = "cneos"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
    "user" : sql_user,
    "password" : sql_password,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read SQL Table

# COMMAND ----------

cneos_data = spark.read.jdbc(url=jdbcUrl, table="COURSE_FEEDBACK", properties=connectionProperties)display(Spdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write SQL Table

# COMMAND ----------

final_df.write.jdbc(url=jdbcUrl, table="CNEOS_DATA", mode = "overwrite", properties=connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Working With SQL Tables

# COMMAND ----------

final_df.createOrReplaceTempView('cneos')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CNEOS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DESIGNATION,
# MAGIC   ORBIT_ID,
# MAGIC   COUNT(CLOSE_APPROACH_DATE_FORMATTED) AS NUMBER_OF_APPROACHES,
# MAGIC   MIN(NOMINAL_APPROCH_DISTANCE_KM) AS MIN_DISTANCE_KM,
# MAGIC   MAX(MAX_CLOSE_APPROACH_DISTANCE_KM) AS MAX_APPROACH_DISTANCE_KM,
# MAGIC   Max(ABSOLUTE_MAGNITUDE_MAG) AS ABSOLUTE_MAGNITUDE_MAG,
# MAGIC   ROUND(MEAN(V_RELETIVE_KMS), 2) AS RELATIVE_VELOCITY_KMS
# MAGIC FROM CNEOS
# MAGIC GROUP BY DESIGNATION, ORBIT_ID
# MAGIC SORT BY NUMBER_OF_APPROACHES DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### SQL Table -- All Asteroid Summerized Data 

# COMMAND ----------

query_asteroid_summary = """
CREATE TABLE IF NOT EXISTS CNEOS_ASTEROID_SUMMARY
(
    SELECT 
      DESIGNATION,
      ORBIT_ID,
      COUNT(CLOSE_APPROACH_DATE_FORMATTED) AS NUMBER_OF_APPROACHES,
      MIN(NOMINAL_APPROCH_DISTANCE_KM) AS MIN_DISTANCE_KM,
      MAX(MAX_CLOSE_APPROACH_DISTANCE_KM) AS MAX_APPROACH_DISTANCE_KM,
      Max(ABSOLUTE_MAGNITUDE_MAG) AS ABSOLUTE_MAGNITUDE_MAG,
      ROUND(MEAN(V_RELETIVE_KMS), 2) AS RELATIVE_VELOCITY_KMS
    FROM CNEOS
    GROUP BY DESIGNATION, ORBIT_ID
    SORT BY NUMBER_OF_APPROACHES DESC
    )"""

# COMMAND ----------

spark.sql(query_asteroid_summary)

# COMMAND ----------

display(spark.sql('select * from cneos_asteroid_summary'))

# COMMAND ----------

spark.sql('SELECT * FROM CNEOS_ASTEROID_SUMMARY').write.jdbc(url=jdbcUrl, table="CNEOS_ASTEROID_SUMMARY", mode = "overwrite", properties=connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### SQL Table -- No of Asteroids in each Orbit

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CNEOS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ORBIT_INFO
# MAGIC (
# MAGIC   SELECT 
# MAGIC     ORBIT_ID AS ORBIT_ID,
# MAGIC     COUNT(DESIGNATION) AS NO_OF_ASTEROIDS,
# MAGIC     ROUND(MEAN(MIN_CLOSE_APPROACH_DISTANCE_KM), 3) AS MEAN_CLOSE_DISTANCE_KM
# MAGIC   FROM CNEOS
# MAGIC   GROUP BY ORBIT_ID
# MAGIC   SORT BY NO_OF_ASTEROIDS DESC
# MAGIC )

# COMMAND ----------

query_orbit_info = '''
CREATE TABLE IF NOT EXISTS ORBIT_INFO
(
  SELECT 
    ORBIT_ID AS ORBIT_ID,
    COUNT(DESIGNATION) AS NO_OF_ASTEROIDS,
    ROUND(MEAN(MIN_CLOSE_APPROACH_DISTANCE_KM), 3) AS MEAN_CLOSE_DISTANCE_KM
  FROM CNEOS
  GROUP BY ORBIT_ID
  SORT BY NO_OF_ASTEROIDS DESC
)
'''

# COMMAND ----------

spark.sql('SELECT * FROM ORBIT_INFO').write.mode('overwrite').jdbc(url=jdbcUrl, table="ORBIT_INFO", mode = "overwrite", properties=connectionProperties)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

temp_table = spark.read.format('jdbc').option('url',url).option('dbtable','TEMP').load()
display(temp_table)

# COMMAND ----------

final_df.write.mode('append').

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

username = dbutils.secrets.get(scope = "AzureKeyVault", key = "freejdbcusername")
password = dbutils.secrets.get(scope = "AzureKeyVault", key = "freejdbcpassword")
tableName = 'CNEOS_Data'
jdbcURL = "jdbc:sqlserver://mydataengineering.database.windows.net:1433;database=DataEngineering;user=" + username + "@mydataengineering;password=" + password + ";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

cneos_table = (spark.read
  .format("jdbc")
  .option("url", jdbcURL)
  .option("dbtable", tableName)
  .option("user", username)
  .option("password", password)
  .load()
)

# COMMAND ----------

(.write
  .format("jdbc")
  .option("url", "<jdbc_url>")
  .option("dbtable", "<new_table_name>")
  .option("user", "<username>")
  .option("password", "<password>")
  .mode("append")
  .save()
)

# COMMAND ----------

final_sdf.createOrReplaceTempView('cneos_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DESIGNATION, 
# MAGIC   COUNT(*) AS OCCURRENCE_COUNT
# MAGIC FROM CNEOS_DATA 
# MAGIC WHERE ORBIT_ID == 6 
# MAGIC GROUP BY DESIGNATION
# MAGIC ORDER BY OCCURRENCE_COUNT DESC

# COMMAND ----------

