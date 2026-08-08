#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql.functions import col, lower, trim, count, to_timestamp, from_utc_timestamp, current_timestamp

# Source: lh_olist_broze / Files / olist_sellers_dataset.csv
# Destination: lh_olist_silver / Tables / silver_sellers

# Grain:
# 1 row = 1 seller
# CSV file path in the Bronze layer
bronze_sellers_path = "abfss://569508d4-c428-48db-8404-4f275c6a813e@onelake.dfs.fabric.microsoft.com/70807d05-5771-492e-8f2f-614ed0f44a9a/Files/olist_sellers_dataset_updated"

# Read sellers data from the Bronze layer
sellers_df = spark.read.csv(
    bronze_sellers_path,
    header=True,
    inferSchema=True
)

# Check the original row count
original_line = sellers_df.count()
print("Original sellers row count:", original_line)

# Remove duplicate rows
sellers_df = sellers_df.dropDuplicates()
print("Row count after removing duplicates:", sellers_df.count())

silver_sellers = sellers_df.select(
    # Seller ID
    col("seller_id"),

    # Seller ZIP code prefix
    col("seller_zip_code_prefix"),

    # Seller city
    lower(trim(col("seller_city"))).alias("seller_city"),

    # Seller state
    col("seller_state"),

    col("insert_date").cast("timestamp").alias("insert_date"),

    # Always update based on the ETL execution time
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    ).alias("update_date")
)

# Write data to the Silver layer as a Delta table
silver_sellers.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_sellers")

print("The silver_sellers table has been created")


# In[3]:


display(
    spark.sql(
        """
        select * from silver_sellers
        """
    )
)

