#!/usr/bin/env python
# coding: utf-8

# In[6]:


from pyspark.sql.functions import (
    col,
    lower,
    trim,
    to_timestamp,
    from_utc_timestamp,
    current_timestamp,
    lit
)

# ============================================================
# SOURCE
# lh_bronze / Files
# ============================================================

BASE_PATH = (
    "abfss://569508d4-c428-48db-8404-4f275c6a813e"
    "@onelake.dfs.fabric.microsoft.com/"
    "70807d05-5771-492e-8f2f-614ed0f44a9a/Files"
)

# Original Olist orders
ORIGINAL_ORDERS_PATH = (
    f"{BASE_PATH}/olist_orders_dataset_updated"
)

# Fake orders for 2019-2026
# IMPORTANT: this must be the ORDERS fake file, NOT customers fake file
FAKE_ORDERS_PATH = (
    f"{BASE_PATH}/olist_orders_dataset_fake_2019_2026_v2.csv"
)

# ============================================================
# DESTINATION
# lh_silver / Tables / silver_orders
# Grain: 1 row = 1 order
# ============================================================


# ------------------------------------------------------------
# 1. READ ORIGINAL ORDERS
# ------------------------------------------------------------

original_orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ORIGINAL_ORDERS_PATH)
)

print("Original orders row count:", original_orders_df.count())


# ------------------------------------------------------------
# 2. READ FAKE ORDERS 2019-2026
# ------------------------------------------------------------

fake_orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(FAKE_ORDERS_PATH)
)

print("Fake orders row count:", fake_orders_df.count())


# ------------------------------------------------------------
# 3. VALIDATE REQUIRED COLUMNS
# ------------------------------------------------------------

required_columns = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

missing_original = [
    c for c in required_columns
    if c not in original_orders_df.columns
]

missing_fake = [
    c for c in required_columns
    if c not in fake_orders_df.columns
]

if missing_original:
    raise ValueError(
        f"Missing columns in original orders file: {missing_original}"
    )

if missing_fake:
    raise ValueError(
        f"Missing columns in fake orders file: {missing_fake}"
    )


# ------------------------------------------------------------
# 4. ADD insert_date IF IT DOES NOT EXIST
# ------------------------------------------------------------

if "insert_date" not in original_orders_df.columns:
    original_orders_df = original_orders_df.withColumn(
        "insert_date",
        from_utc_timestamp(
            current_timestamp(),
            "America/New_York"
        )
    )

if "insert_date" not in fake_orders_df.columns:
    fake_orders_df = fake_orders_df.withColumn(
        "insert_date",
        from_utc_timestamp(
            current_timestamp(),
            "America/New_York"
        )
    )


# ------------------------------------------------------------
# 5. SELECT THE SAME COLUMNS BEFORE UNION
# ------------------------------------------------------------

columns_to_keep = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "insert_date"
]

original_orders_df = original_orders_df.select(
    *columns_to_keep
)

fake_orders_df = fake_orders_df.select(
    *columns_to_keep
)


# ------------------------------------------------------------
# 6. UNION BY COLUMN NAME
# ------------------------------------------------------------

orders_df = original_orders_df.unionByName(
    fake_orders_df,
    allowMissingColumns=False
)

print("Combined orders row count:", orders_df.count())


# ------------------------------------------------------------
# 7. REMOVE DUPLICATES
# ------------------------------------------------------------

orders_df = orders_df.dropDuplicates(["order_id"])

print(
    "Row count after removing duplicate order_id:",
    orders_df.count()
)


# ------------------------------------------------------------
# 8. TRANSFORM TO SILVER SCHEMA
# ------------------------------------------------------------

silver_orders = orders_df.select(

    col("order_id").cast("string").alias("order_id"),

    col("customer_id").cast("string").alias("customer_id"),

    lower(
        trim(col("order_status"))
    ).alias("order_status"),

    to_timestamp(
        col("order_purchase_timestamp")
    ).alias("order_purchase_timestamp"),

    to_timestamp(
        col("order_approved_at")
    ).alias("order_approved_at"),

    to_timestamp(
        col("order_delivered_carrier_date")
    ).alias("order_delivered_carrier_date"),

    to_timestamp(
        col("order_delivered_customer_date")
    ).alias("order_delivered_customer_date"),

    to_timestamp(
        col("order_estimated_delivery_date")
    ).alias("order_estimated_delivery_date"),

    col("insert_date")
        .cast("timestamp")
        .alias("insert_date"),

    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    ).alias("update_date")
)


# ------------------------------------------------------------
# 9. WRITE TO SILVER
# ------------------------------------------------------------

silver_orders.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_orders")

print("The silver_orders table has been created successfully.")
print(
    "Total silver_orders row count:",
    spark.table("silver_orders").count()
)


# ============================================================
# 10. VALIDATION - ORDERS BY YEAR
# ============================================================

display(
    spark.sql("""
        SELECT
            YEAR(order_purchase_timestamp) AS year,
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_id) AS order_count
        FROM silver_orders
        GROUP BY YEAR(order_purchase_timestamp)
        ORDER BY year
    """)
)


# ============================================================
# 11. VALIDATION - ORDER STATUS
# ============================================================

display(
    spark.sql("""
        SELECT
            order_status,
            COUNT(*) AS order_count
        FROM silver_orders
        GROUP BY order_status
        ORDER BY order_count DESC
    """)
)


# ============================================================
# 12. VALIDATION - NULL CHECK
# ============================================================

display(
    spark.sql("""
        SELECT
            COUNT(*) AS total_rows,

            SUM(
                CASE
                    WHEN order_purchase_timestamp IS NULL
                    THEN 1 ELSE 0
                END
            ) AS null_purchase_timestamp,

            SUM(
                CASE
                    WHEN customer_id IS NULL
                    THEN 1 ELSE 0
                END
            ) AS null_customer_id,

            SUM(
                CASE
                    WHEN order_status IS NULL
                    THEN 1 ELSE 0
                END
            ) AS null_order_status

        FROM silver_orders
    """)
)


# In[8]:


display(
    spark.sql("""
        SELECT
            order_status,
            COUNT(*) AS order_count
        FROM lh_silver.dbo.silver_orders
        GROUP BY order_status
        ORDER BY order_count DESC
    """)
)


# In[7]:


display(
    spark.sql("""
        SELECT
            YEAR(order_purchase_timestamp) AS year,
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_id) AS order_count
        FROM lh_silver.dbo.silver_orders
        GROUP BY YEAR(order_purchase_timestamp)
        ORDER BY year
    """)
)

