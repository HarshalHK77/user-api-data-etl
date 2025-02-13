# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

import requests
import json

# Initialize Spark session
spark = SparkSession.builder.appName("Fetch and Flatten JSON").getOrCreate()

# Fetch data from API
api_url = "https://randomuser.me/api/?results=250"
response = requests.get(api_url)

if response.status_code == 200:
    raw_data = response.text  # Get raw JSON text
else:
    raise Exception("Error fetching data:", response.status_code)



# COMMAND ----------

# Create DataFrame from JSON string
df_raw = spark.read.json(spark.sparkContext.parallelize([raw_data]))

# Explode 'results' array to separate rows
df_exploded = df_raw.select(explode(col("results")).alias("user"))

# Flatten nested fields using dot notation
df = df_exploded.select(
    col("user.gender").alias("gender"),
    col("user.name.title").alias("title"),
    col("user.name.first").alias("first_name"),
    col("user.name.last").alias("last_name"),
    col("user.location.street.number").alias("street_number"),
    col("user.location.street.name").alias("street_name"),
    col("user.location.city").alias("city"),
    col("user.location.state").alias("state"),
    col("user.location.country").alias("country"),
    col("user.location.postcode").alias("postcode"),
    col("user.location.coordinates.latitude").alias("latitude"),
    col("user.location.coordinates.longitude").alias("longitude"),
    col("user.location.timezone.offset").alias("timezone_offset"),
    col("user.location.timezone.description").alias("timezone_desc"),
    col("user.email").alias("email"),
    col("user.login.username").alias("username"),
    col("user.login.password").alias("password"),
    col("user.dob.date").alias("dob_date"),
    col("user.dob.age").alias("dob_age"),
    col("user.cell").alias("cell"),
    col("user.picture.large").alias("picture_large"),
    col("user.picture.medium").alias("picture_medium"),
    col("user.nat").alias("nationality")
)

# Show DataFrame schema and count
df.printSchema()
print("Total Records:", df.count())

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn("street_number", col("street_number").cast("int")) \
       .withColumn("latitude", col("latitude").cast("double")) \
       .withColumn("longitude", col("longitude").cast("double")) \
       .withColumn("dob_date", col("dob_date").cast("date")) \
       .withColumn("dob_age", col("dob_age").cast("int"))


# COMMAND ----------

df.count()
df.display()

# COMMAND ----------

spark._jsc.hadoopConfiguration().set("fs.s3.access.key", "AKIAUMYCIDRWZMGEJ4XL")
spark._jsc.hadoopConfiguration().set("fs.s3.secret.key", "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")
spark._jsc.hadoopConfiguration().set("fs.s3.endpoint", "s3.amazonaws.com")


# COMMAND ----------

# MAGIC %md
# MAGIC ### add this to cluster
# MAGIC
# MAGIC spark.hadoop.fs.s3.access.key "AKIAUMYCIDRWZMGEJ4XL"
# MAGIC
# MAGIC spark.hadoop.fs.s3.secret.key "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"
# MAGIC
# MAGIC spark.hadoop.fs.s3a.endpoint s3.amazonaws.com

# COMMAND ----------

from datetime import datetime
import boto3
import os
import shutil

# Generate filename
filename = f"random_users_{datetime.now().strftime('%d_%m_%Y_%H_%M_%S')}.csv"

# Temporary path in DBFS
dbfs_output_path = "dbfs:/FileStore/tables/CSV/random_output"

# Write DataFrame to DBFS
df.coalesce(1) \
    .write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(dbfs_output_path)

# Move files from DBFS to local for renaming
local_temp_path = "/tmp/random_output"
dbutils.fs.cp(dbfs_output_path, f"file://{local_temp_path}", recurse=True)

# Find and rename the part file locally
for file in os.listdir(local_temp_path):
    if file.startswith("part-"):
        os.rename(os.path.join(local_temp_path, file), os.path.join(local_temp_path, filename))
        break

# Upload the renamed CSV file to S3

s3 = boto3.client('s3',
                  aws_access_key_id='AKIAUMYCIDRWZMGEJ4XL',
                  aws_secret_access_key='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')

s3.upload_file(os.path.join(local_temp_path, filename), "user-api-data-databricks", f"csv/{filename}")


print("Single CSV file written to S3 successfully!")

