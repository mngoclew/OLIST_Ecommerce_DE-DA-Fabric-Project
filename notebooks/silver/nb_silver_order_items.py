#!/usr/bin/env python
# coding: utf-8

# In[4]:


from pyspark.sql.functions import (
    col,
    to_timestamp,
    from_utc_timestamp,
    current_timestamp,
    year,
    sequence,
    explode,
    lit,
    pmod,
    xxhash64,
    row_number,
    expr
)

from pyspark.sql.window import Window


# ============================================================
# SOURCE
# lh_bronze / Files / olist_order_items_dataset_updated
# ============================================================

ORDER_ITEMS_PATH = (
    "abfss://569508d4-c428-48db-8404-4f275c6a813e"
    "@onelake.dfs.fabric.microsoft.com/"
    "70807d05-5771-492e-8f2f-614ed0f44a9a/"
    "Files/olist_order_items_dataset_updated"
)


# ============================================================
# DESTINATION
# lh_silver / Tables / silver_order_items
# Grain: 1 row = 1 product in 1 order
# ============================================================


# ============================================================
# 1. READ ORIGINAL ORDER ITEMS FROM BRONZE
# ============================================================

order_items_df = spark.read.csv(
    ORDER_ITEMS_PATH,
    header=True,
    inferSchema=True
)

print(
    "Original order items row count:",
    order_items_df.count()
)


# ============================================================
# 2. REMOVE DUPLICATES
# ============================================================

order_items_df = order_items_df.dropDuplicates()

print(
    "Row count after removing duplicates:",
    order_items_df.count()
)


# ============================================================
# 3. CLEAN ORIGINAL ORDER ITEMS
# ============================================================

original_order_items = order_items_df.select(

    # Order ID
    col("order_id")
        .cast("string")
        .alias("order_id"),

    # Product sequence number within the order
    col("order_item_id")
        .cast("int")
        .alias("order_item_id"),

    # Product ID
    col("product_id")
        .cast("string")
        .alias("product_id"),

    # Seller ID
    col("seller_id")
        .cast("string")
        .alias("seller_id"),

    # Seller shipping deadline
    to_timestamp(
        col("shipping_limit_date")
    ).alias("shipping_limit_date"),

    # Product price
    col("price")
        .cast("double")
        .alias("price"),

    # Freight value
    col("freight_value")
        .cast("double")
        .alias("freight_value"),

    # Original insert date
    col("insert_date")
        .cast("timestamp")
        .alias("insert_date"),

    # ETL update time - New York timezone
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    ).alias("update_date")
)

print(
    "Clean original order items row count:",
    original_order_items.count()
)


# ============================================================
# 4. READ FAKE ORDERS 2019-2026 FROM SILVER ORDERS
# ============================================================

fake_orders = (
    spark.table("lh_silver.dbo.silver_orders")

    .filter(
        (year(col("order_purchase_timestamp")) >= 2019) &
        (year(col("order_purchase_timestamp")) <= 2026)
    )

    .select(
        col("order_id").cast("string").alias("order_id"),
        col("order_purchase_timestamp")
    )

    .dropDuplicates(["order_id"])
)

fake_orders_count = fake_orders.count()

print(
    "Fake orders 2019-2026:",
    fake_orders_count
)


# ============================================================
# 5. CREATE HISTORICAL ORDER ITEM TEMPLATES
# ============================================================
# Reuse real Olist product / seller / price / freight
# combinations to generate realistic fake order items.
# ============================================================

template_window = Window.orderBy(
    "product_id",
    "seller_id",
    "price",
    "freight_value"
)

templates = (
    original_order_items

    .filter(
        col("product_id").isNotNull() &
        col("seller_id").isNotNull() &
        col("price").isNotNull() &
        col("freight_value").isNotNull()
    )

    .select(
        "product_id",
        "seller_id",
        "price",
        "freight_value"
    )

    .withColumn(
        "template_id",
        row_number().over(template_window)
    )
)

template_count = templates.count()

print(
    "Available historical templates:",
    template_count
)


# Stop if source data is missing
if fake_orders_count == 0:
    raise ValueError(
        "No fake orders from 2019-2026 were found in silver_orders."
    )

if template_count == 0:
    raise ValueError(
        "No valid historical order item templates were found."
    )


# ============================================================
# 6. GENERATE 1-3 ITEMS FOR EACH FAKE ORDER
# ============================================================

fake_order_items_base = (
    fake_orders

    # Each fake order contains between 1 and 3 items
    .withColumn(
        "item_count",
        (
            pmod(
                xxhash64(col("order_id")),
                lit(3)
            ) + lit(1)
        ).cast("int")
    )

    # Generate order_item_id = 1, 2 or 3
    .withColumn(
        "order_item_id",
        explode(
            sequence(
                lit(1),
                col("item_count")
            )
        )
    )

    # Assign a historical product/seller template
    .withColumn(
        "template_id",
        (
            pmod(
                xxhash64(
                    col("order_id"),
                    col("order_item_id")
                ),
                lit(template_count)
            ) + lit(1)
        ).cast("int")
    )
)


# ============================================================
# 7. CREATE FAKE ORDER ITEMS
# ============================================================

fake_order_items = (
    fake_order_items_base.alias("fo")

    .join(
        templates.alias("t"),
        col("fo.template_id") == col("t.template_id"),
        "inner"
    )

    .select(

        # Must match fake order_id in silver_orders
        col("fo.order_id")
            .cast("string")
            .alias("order_id"),

        # Sequence within each order
        col("fo.order_item_id")
            .cast("int")
            .alias("order_item_id"),

        # Real historical product
        col("t.product_id")
            .cast("string")
            .alias("product_id"),

        # Real historical seller
        col("t.seller_id")
            .cast("string")
            .alias("seller_id"),

        # Shipping limit = purchase date + 5 days
        expr(
            "fo.order_purchase_timestamp + INTERVAL 5 DAYS"
        ).alias("shipping_limit_date"),

        # Historical product price
        col("t.price")
            .cast("double")
            .alias("price"),

        # Historical freight value
        col("t.freight_value")
            .cast("double")
            .alias("freight_value"),

        # Insert timestamp
        from_utc_timestamp(
            current_timestamp(),
            "America/New_York"
        ).alias("insert_date"),

        # Update timestamp
        from_utc_timestamp(
            current_timestamp(),
            "America/New_York"
        ).alias("update_date")
    )
)

fake_order_items_count = fake_order_items.count()

print(
    "Generated fake order items:",
    fake_order_items_count
)


# ============================================================
# 8. COMBINE ORIGINAL + FAKE ORDER ITEMS
# ============================================================

silver_order_items = (
    original_order_items

    .unionByName(
        fake_order_items,
        allowMissingColumns=False
    )

    # Grain:
    # 1 order_id + 1 order_item_id = 1 unique order item
    .dropDuplicates(
        ["order_id", "order_item_id"]
    )
)

final_count = silver_order_items.count()

print(
    "Final silver_order_items row count:",
    final_count
)


# ============================================================
# 9. WRITE TO SILVER
# ============================================================

silver_order_items.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_order_items")

print(
    "The silver_order_items table has been created successfully."
)

print(
    "Total silver_order_items row count:",
    spark.table("silver_order_items").count()
)


# ============================================================
# 10. VALIDATION - ORDERS WITH ITEMS BY YEAR
# ============================================================

display(
    spark.sql("""
        SELECT
            YEAR(o.order_purchase_timestamp) AS year,

            COUNT(DISTINCT o.order_id) AS total_orders,

            COUNT(DISTINCT oi.order_id) AS orders_with_items,

            COUNT(oi.order_id) AS total_item_rows

        FROM lh_silver.dbo.silver_orders o

        LEFT JOIN lh_silver.dbo.silver_order_items oi
            ON o.order_id = oi.order_id

        GROUP BY
            YEAR(o.order_purchase_timestamp)

        ORDER BY
            year
    """)
)


# ============================================================
# 11. VALIDATION - NULL CHECK
# ============================================================

display(
    spark.sql("""
        SELECT
            COUNT(*) AS total_rows,

            SUM(
                CASE
                    WHEN order_id IS NULL
                    THEN 1 ELSE 0
                END
            ) AS null_order_id,

            SUM(
                CASE
                    WHEN product_id IS NULL
                    THEN 1 ELSE 0
                END
            ) AS null_product_id,

            SUM(
                CASE
                    WHEN seller_id IS NULL
                    THEN 1 ELSE 0
                END
            ) AS null_seller_id,

            SUM(
                CASE
                    WHEN price IS NULL
                    THEN 1 ELSE 0
                END
            ) AS null_price

        FROM lh_silver.dbo.silver_order_items
    """)
)


# ============================================================
# 12. VALIDATION - SAMPLE FAKE ORDER ITEMS 2019-2026
# ============================================================

display(
    spark.sql("""
        SELECT
            YEAR(o.order_purchase_timestamp) AS year,
            oi.order_id,
            oi.order_item_id,
            oi.product_id,
            oi.seller_id,
            oi.price,
            oi.freight_value,
            oi.shipping_limit_date

        FROM lh_silver.dbo.silver_order_items oi

        INNER JOIN lh_silver.dbo.silver_orders o
            ON oi.order_id = o.order_id

        WHERE YEAR(o.order_purchase_timestamp)
            BETWEEN 2019 AND 2026

        ORDER BY
            year,
            oi.order_id,
            oi.order_item_id

        LIMIT 50
    """)
)


# In[1]:


spark.table("lh_silver.dbo.silver_order_items").printSchema()


# In[2]:


from pyspark.sql.functions import (
    col,
    year,
    sequence,
    explode,
    lit,
    pmod,
    xxhash64,
    row_number,
    expr,
    current_timestamp,
    from_utc_timestamp
)

from pyspark.sql.window import Window


# ============================================================
# 1. READ CURRENT SILVER DATA
# ============================================================

orders_df = spark.table("lh_silver.dbo.silver_orders")

order_items_df = spark.table("lh_silver.dbo.silver_order_items")


# ============================================================
# 2. GET FAKE ORDERS 2019-2026
# ============================================================

fake_orders_df = (
    orders_df
    .filter(
        (year(col("order_purchase_timestamp")) >= 2019) &
        (year(col("order_purchase_timestamp")) <= 2026)
    )
    .select(
        "order_id",
        "order_purchase_timestamp"
    )
)

print(
    "Fake orders 2019-2026:",
    fake_orders_df.count()
)


# ============================================================
# 3. USE ORIGINAL ORDER ITEMS AS REALISTIC TEMPLATES
# ============================================================

template_window = Window.orderBy(
    "product_id",
    "seller_id",
    "price",
    "freight_value"
)

templates_df = (
    order_items_df
    .select(
        "product_id",
        "seller_id",
        "price",
        "freight_value"
    )
    .filter(
        col("product_id").isNotNull() &
        col("seller_id").isNotNull() &
        col("price").isNotNull()
    )
    .withColumn(
        "template_id",
        row_number().over(template_window)
    )
)

template_count = templates_df.count()

print(
    "Available order item templates:",
    template_count
)


# ============================================================
# 4. GENERATE 1-3 ITEMS FOR EACH FAKE ORDER
# ============================================================

fake_order_items_base = (
    fake_orders_df

    # Deterministic number of products per order: 1-3
    .withColumn(
        "item_count",
        pmod(
            xxhash64(col("order_id")),
            lit(3)
        ) + lit(1)
    )

    # Create order_item_id = 1, 2, 3...
    .withColumn(
        "order_item_id",
        explode(
            sequence(
                lit(1),
                col("item_count")
            )
        )
    )

    # Deterministically choose a real historical item template
    .withColumn(
        "template_id",
        pmod(
            xxhash64(
                col("order_id"),
                col("order_item_id")
            ),
            lit(template_count)
        ) + lit(1)
    )
)


# ============================================================
# 5. ADD PRODUCT / SELLER / PRICE / FREIGHT
# ============================================================

fake_order_items = (
    fake_order_items_base.alias("fo")

    .join(
        templates_df.alias("t"),
        col("fo.template_id") == col("t.template_id"),
        "inner"
    )

    .select(
        col("fo.order_id").alias("order_id"),

        col("fo.order_item_id")
            .cast("int")
            .alias("order_item_id"),

        col("t.product_id").alias("product_id"),

        col("t.seller_id").alias("seller_id"),

        # Shipping limit = purchase date + 5 days
        expr(
            "fo.order_purchase_timestamp + INTERVAL 5 DAYS"
        ).alias("shipping_limit_date"),

        col("t.price")
            .cast("double")
            .alias("price"),

        col("t.freight_value")
            .cast("double")
            .alias("freight_value"),

        from_utc_timestamp(
            current_timestamp(),
            "America/New_York"
        ).alias("insert_date"),

        from_utc_timestamp(
            current_timestamp(),
            "America/New_York"
        ).alias("update_date")
    )
)


print(
    "Generated fake order items:",
    fake_order_items.count()
)

display(fake_order_items.limit(20))


# In[3]:


display(
    fake_order_items.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value",
        "shipping_limit_date"
    ).limit(20)
)

