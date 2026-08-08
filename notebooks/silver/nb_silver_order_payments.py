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

# Source: lh_olist_bronze / Files / olist_order_payments_dataset.csv

# Destination: lh_olist_silver / Tables / silver_payments

# Grain: 1 row = 1 payment transaction for 1 order

# One order_id may have multiple payment records
# Because a customer may pay in multiple transactions or use different payment methods
# Each payment method may also have a specific number of installments

payments_df = spark.read.csv(
    "abfss://569508d4-c428-48db-8404-4f275c6a813e@onelake.dfs.fabric.microsoft.com/70807d05-5771-492e-8f2f-614ed0f44a9a/Files/olist_order_payments_dataset_updated",
    header=True,
    inferSchema=True
)
print("Original table row count: ", payments_df.count())

payments_df = payments_df.dropDuplicates()
print("Row count after removing duplicates: ", payments_df.count())

silver_payments = payments_df.select(
    # Order ID
    col("order_id"),

    # Payment sequence within the order
    col("payment_sequential"),

    # Payment method by payment sequence within the order
    lower(
        trim(col("payment_type"))
    ).alias("payment_type"),

    # Number of installments by payment sequence
    col("payment_installments")
        .cast("int")
        .alias("payment_installments"),

    # Payment value
    col("payment_value")
        .cast("double")
        .alias("payment_value"),

    col("insert_date")
        .cast("timestamp")
        .alias("insert_date"),

    # Always update based on the ETL execution time
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    ).alias("update_date")
)

silver_payments.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_payments")

print("The silver_payments table has been created successfully")


# In[2]:


display (
    spark.sql (
        """
            select * 
            from silver_payments sp
            where sp.order_id = '0016dfedd97fc2950e388d2971d718c7'
            or sp.order_id = '009ac365164f8e06f59d18a08045f6c4'
        """
    )
)

