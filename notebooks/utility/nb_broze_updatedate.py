#!/usr/bin/env python
# coding: utf-8

# In[4]:


from pyspark.sql import functions as F
from datetime import datetime
from zoneinfo import ZoneInfo

base_path = "Files"

Files = [
    "olist_customers_dataset",
    "olist_orders_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "olist_geolocation_dataset",
    "product_category_name_translation"
]

# US time
us_now = datetime.now(
    ZoneInfo("America/New_York")
).strftime("%Y-%m-%d %H:%M:%S")

for file_name in files:

    input_path = f"{base_path}/{file_name}.csv"
    output_path = f"{base_path}/{file_name}_updated"

    print("Processing:", file_name)

    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )

    print("Original row count:", df.count())

    df = df \
        .withColumn("insert_date", F.lit(us_now)) \
        .withColumn("update_date", F.lit(us_now))

    print("Row count after adding insert_date and update_date:", df.count())

    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(output_path)

    print("Data saved to:", output_path)
    print("====================================")

print("Completed adding insert_date and update_date to all Broze files")

