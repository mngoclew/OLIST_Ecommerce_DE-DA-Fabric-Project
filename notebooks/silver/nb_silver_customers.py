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

# ============================================================
# SOURCE
# lh_bronze / Files
# ============================================================

BASE_PATH = (
    "abfss://569508d4-c428-48db-8404-4f275c6a813e"
    "@onelake.dfs.fabric.microsoft.com/"
    "70807d05-5771-492e-8f2f-614ed0f44a9a/Files"
)

ORIGINAL_CUSTOMERS_PATH = (
    f"{BASE_PATH}/olist_customers_dataset_updated"
)

FAKE_CUSTOMERS_PATH = (
    f"{BASE_PATH}/olist_customers_dataset_fake_2019_2026_v2.csv"
)


# ============================================================
# 1. READ ORIGINAL CUSTOMERS
# ============================================================

original_customers_df = spark.read.csv(
    ORIGINAL_CUSTOMERS_PATH,
    header=True,
    inferSchema=True
)

print(
    "Original customers row count:",
    original_customers_df.count()
)


# ============================================================
# 2. READ FAKE CUSTOMERS 2019-2026
# ============================================================

fake_customers_df = spark.read.csv(
    FAKE_CUSTOMERS_PATH,
    header=True,
    inferSchema=True
)

print(
    "Fake customers row count:",
    fake_customers_df.count()
)


# ============================================================
# 3. VALIDATE REQUIRED COLUMNS
# ============================================================

required_columns = [
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state"
]

missing_original = [
    c for c in required_columns
    if c not in original_customers_df.columns
]

missing_fake = [
    c for c in required_columns
    if c not in fake_customers_df.columns
]

if missing_original:
    raise ValueError(
        f"Missing columns in original customers: {missing_original}"
    )

if missing_fake:
    raise ValueError(
        f"Missing columns in fake customers: {missing_fake}"
    )


# ============================================================
# 4. ADD insert_date IF MISSING
# ============================================================

if "insert_date" not in original_customers_df.columns:
    original_customers_df = (
        original_customers_df
        .withColumn(
            "insert_date",
            from_utc_timestamp(
                current_timestamp(),
                "America/New_York"
            )
        )
    )

if "insert_date" not in fake_customers_df.columns:
    fake_customers_df = (
        fake_customers_df
        .withColumn(
            "insert_date",
            from_utc_timestamp(
                current_timestamp(),
                "America/New_York"
            )
        )
    )


# ============================================================
# 5. STANDARDIZE ORIGINAL CUSTOMERS
# ============================================================

original_customers = original_customers_df.select(

    col("customer_id")
        .cast("string")
        .alias("customer_id"),

    col("customer_unique_id")
        .cast("string")
        .alias("customer_unique_id"),

    col("customer_zip_code_prefix")
        .cast("string")
        .alias("customer_zip_code_prefix"),

    lower(
        trim(col("customer_city"))
    ).alias("customer_city"),

    trim(
        col("customer_state")
    ).alias("customer_state"),

    col("insert_date")
        .cast("timestamp")
        .alias("insert_date")
)


# ============================================================
# 6. STANDARDIZE FAKE CUSTOMERS
# ============================================================

fake_customers = fake_customers_df.select(

    col("customer_id")
        .cast("string")
        .alias("customer_id"),

    col("customer_unique_id")
        .cast("string")
        .alias("customer_unique_id"),

    col("customer_zip_code_prefix")
        .cast("string")
        .alias("customer_zip_code_prefix"),

    lower(
        trim(col("customer_city"))
    ).alias("customer_city"),

    trim(
        col("customer_state")
    ).alias("customer_state"),

    col("insert_date")
        .cast("timestamp")
        .alias("insert_date")
)


# ============================================================
# 7. UNION ORIGINAL + FAKE CUSTOMERS
# ============================================================

customers_df = (
    original_customers
    .unionByName(
        fake_customers,
        allowMissingColumns=False
    )
    .dropDuplicates(["customer_id"])
)

print(
    "Combined customer row count:",
    customers_df.count()
)


# ============================================================
# 8. CREATE SILVER CUSTOMERS
# ============================================================

silver_customers = (
    customers_df
    .withColumn(
        "update_date",
        from_utc_timestamp(
            current_timestamp(),
            "America/New_York"
        )
    )
)


# ============================================================
# 9. WRITE TO SILVER
# ============================================================

silver_customers.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_customers")

print(
    "The silver_customers table has been created successfully."
)

print(
    "Total silver_customers row count:",
    spark.table("silver_customers").count()
)


# In[2]:


display(
    spark.sql("""
        SELECT
            YEAR(o.order_purchase_timestamp) AS year,
            COUNT(DISTINCT o.order_id) AS total_orders,
            COUNT(DISTINCT c.customer_id) AS matched_customers
        FROM lh_silver.dbo.silver_orders o

        LEFT JOIN lh_silver.dbo.silver_customers c
            ON o.customer_id = c.customer_id

        GROUP BY YEAR(o.order_purchase_timestamp)
        ORDER BY year
    """)
)


# In[2]:


display(
    spark.sql("""
        select * from silver_customers
        limit 10
    """)
)


# In[3]:


display(
    spark.sql("""
        select
            max(update_date) as max_update_date
        from silver_customers
    """)
)

