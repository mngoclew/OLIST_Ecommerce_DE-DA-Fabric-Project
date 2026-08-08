#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import col, lower, trim, count, from_utc_timestamp, current_timestamp
# Source: lh_olist_broze / Files / olist_products_dataset.csv

# Destination: lh_olist_silver / Table / silver_products

# Grain: 1 row = 1 product

# Read data from the Bronze layer
products_df = spark.read.csv(
    "abfss://569508d4-c428-48db-8404-4f275c6a813e@onelake.dfs.fabric.microsoft.com/70807d05-5771-492e-8f2f-614ed0f44a9a/Files/olist_products_dataset_updated",
    header=True,
    inferSchema=True
)
 # Check the original data
original_line = products_df.count()

print("Original row count: ", original_line)

# Remove duplicate records
products_df = products_df.dropDuplicates()
print("Row count after cleaning: ", products_df.count())
# Standardize and rename columns
silver_products = products_df.select (
    # Product ID
    col("product_id"),

    # Product category
    lower(trim(col("product_category_name"))).alias("product_category_name"),

    # Product name length
    col("product_name_lenght"),

    # Product description length
    col("product_description_lenght"),

    # Number of product images
    col("product_photos_qty"),

    # Product weight in grams
    col("product_weight_g").cast("double"),

    # Product length
    col("product_length_cm").cast("double"),

    # Product width
    col("product_width_cm").cast("double"),

    # Product height
    col("product_height_cm").cast("double"),

    col("insert_date").cast("timestamp").alias("insert_date"),
    # Always update based on the ETL execution time
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    ).alias("update_date")
)

# Write data to the Silver layer
silver_products.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_products")

print("The silver_products table has been created")


# In[2]:


display(
    spark.sql("""
        SELECT *
        FROM silver_products
        limit 10
    """)
)

