#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import (
    col,
    lower,
    trim,
    from_utc_timestamp,
    current_timestamp
)

# Source: lh_olist_bronze / Files / olist_geolocation_dataset.csv

# Destination: lh_olist_silver / Tables / silver_geolocation

# Grain: 1 row = 1 geographical location

geolocation_df = spark.read.csv(
    "abfss://569508d4-c428-48db-8404-4f275c6a813e@onelake.dfs.fabric.microsoft.com/70807d05-5771-492e-8f2f-614ed0f44a9a/Files/olist_geolocation_dataset_updated",
    header=True,
    inferSchema=True
)

print("Original table row count:", geolocation_df.count())

geolocation_df = geolocation_df.dropDuplicates()

print("Row count after removing duplicates:", geolocation_df.count())

silver_geolocation = geolocation_df.select(
    # ZIP code prefix
    col("geolocation_zip_code_prefix")
        .cast("int")
        .alias("geolocation_zip_code_prefix"),

    # Latitude
    col("geolocation_lat")
        .cast("double")
        .alias("geolocation_lat"),

    # Longitude
    col("geolocation_lng")
        .cast("double")
        .alias("geolocation_lng"),

    # City
    lower(
        trim(col("geolocation_city"))
    ).alias("geolocation_city"),

    # State
    col("geolocation_state").alias("geolocation_state"),

    col("insert_date")
        .cast("timestamp")
        .alias("insert_date"),

    # Always update based on the ETL execution time
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    ).alias("update_date")
)

silver_geolocation.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_geolocation")

print("The silver_geolocation table has been created successfully")

