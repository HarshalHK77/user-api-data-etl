from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

import requests
import json
import boto3
import os
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("Fetch and Flatten JSON").getOrCreate()

# Fetch data from API
api_url = "https://randomuser.me/api/?results=250"
response = requests.get(api_url)

if response.status_code == 200:
    raw_data = response.text  # Get raw JSON text
else:
    raise Exception("Error fetching data:", response.status_code)

# Create DataFrame from JSON string
df_raw = spark.read.json(spark.sparkContext.parallelize([raw_data]))

df_exploded = df_raw.select(explode(col("results")).alias("user"))

personal_info = df_exploded.select(
    col("user.gender").alias("gender"),
    col("user.name.title").alias("title"),
    col("user.name.first").alias("first_name"),
    col("user.name.last").alias("last_name"),
    col("user.dob.date").alias("dob_date"),
    col("user.dob.age").alias("dob_age"),
    col("user.email").alias("email"),
    col("user.cell").alias("cell"),
    col("user.nat").alias("nationality")
)

location_info = df_exploded.select(
    col("user.location.street.number").alias("street_number"),
    col("user.location.street.name").alias("street_name"),
    col("user.location.city").alias("city"),
    col("user.location.state").alias("state"),
    col("user.location.country").alias("country"),
    col("user.location.postcode").alias("postcode"),
    col("user.location.coordinates.latitude").alias("latitude"),
    col("user.location.coordinates.longitude").alias("longitude"),
    col("user.location.timezone.offset").alias("timezone_offset"),
    col("user.location.timezone.description").alias("timezone_desc")
)

login_info = df_exploded.select(
    col("user.login.username").alias("username"),
    col("user.login.password").alias("password")
)

picture_info = df_exploded.select(
    col("user.picture.large").alias("picture_large"),
    col("user.picture.medium").alias("picture_medium")
)

df = personal_info.join(location_info, how='inner').join(login_info, how='inner').join(picture_info, how='inner')

# Standardize Data Types
df = df.withColumn("street_number", col("street_number").cast("int")) \
       .withColumn("latitude", col("latitude").cast("double")) \
       .withColumn("longitude", col("longitude").cast("double")) \
       .withColumn("dob_date", col("dob_date").cast("date")) \
       .withColumn("dob_age", col("dob_age").cast("int"))

# Handle Missing Values
df = df.fillna({
    "gender": "Unknown",
    "street_number": 0,
    "street_name": "N/A",
    "city": "N/A",
    "state": "N/A",
    "country": "N/A",
    "postcode": 0,
    "latitude": 0.0,
    "longitude": 0.0,
    "timezone_offset": "N/A",
    "timezone_desc": "N/A",
    "email": "N/A",
    "username": "N/A",
    "password": "N/A",
    "dob_date": "1900-01-01",
    "dob_age": 0,
    "cell": "N/A",
    "picture_large": "N/A",
    "picture_medium": "N/A",
    "nationality": "N/A"
})

# Normalize Data
df = df.withColumn("gender", lower(trim(col("gender"))))
df = df.withColumn("city", lower(trim(col("city"))))
df = df.withColumn("state", lower(trim(col("state"))))

# Encrypt Sensitive Fields
df = df.withColumn("password", sha2(col("password"), 256))

# Add Derived Columns
df = df.withColumn("full_name", concat_ws(" ", col("title"), col("first_name"), col("last_name")))
df = df.withColumn("age_group", when(col("dob_age") < 18, "Minor")
                                  .when((col("dob_age") >= 18) & (col("dob_age") < 60), "Adult")
                                  .otherwise("Senior"))

# Optimize Performance
df = df.repartition("country")

# Generate filename
filename = f"random_users_{datetime.now().strftime('%d_%m_%Y_%H_%M_%S')}.csv"

# Write DataFrame to S3 path
s3_output_path = f"s3a://user-api-data-databricks/csv/{filename}"
s3 = boto3.client('s3',
                  aws_access_key_id='AKIAUMYCIDRWZMGEJ4XL',
                  aws_secret_access_key='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')

s3.upload_file(filename)
print("Single CSV file written to S3 successfully!")