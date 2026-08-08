#!/usr/bin/env python
# coding: utf-8

# In[5]:


from pyspark.sql.functions import from_utc_timestamp, current_timestamp

# Grain: 1 row = 1 product from 1 seller within 1 order
fact_sales = spark.sql(
    """
        SELECT
            CAST(
                DATE_FORMAT(
                    TO_DATE(so.order_purchase_timestamp),
                    'yyyyMMdd'
                ) AS INT
            ) AS date_key,

            sc.customer_unique_id AS customer_id,
            so.order_id,
            soi.product_id,
            soi.seller_id,

            COUNT(soi.order_item_id) AS product_quantity,

            CASE so.order_status
                WHEN 'created' THEN 'Created'
                WHEN 'approved' THEN 'Approved'
                WHEN 'invoiced' THEN 'Invoiced'
                WHEN 'processing' THEN 'Processing'
                WHEN 'shipped' THEN 'Shipped'
                WHEN 'delivered' THEN 'Delivered'
                WHEN 'canceled' THEN 'Canceled'
                WHEN 'unavailable' THEN 'Unavailable'
                ELSE 'Other'
            END AS order_status,

            soi.price AS seller_unit_price,

            SUM(soi.price) AS ordered_product_value,

            SUM(soi.freight_value) AS product_shipping_cost,

            SUM(soi.price) + SUM(soi.freight_value)
                AS total_product_order_value,

            CASE
                WHEN so.order_status = 'delivered'
                    THEN SUM(soi.price)
                ELSE 0
            END AS recognized_revenue,

            so.order_purchase_timestamp,
            so.insert_date AS insert_date

        FROM lh_silver.dbo.silver_orders so

        JOIN lh_silver.dbo.silver_customers sc
            ON so.customer_id = sc.customer_id

        JOIN lh_silver.dbo.silver_order_items soi
            ON so.order_id = soi.order_id

        GROUP BY
            CAST(
                DATE_FORMAT(
                    TO_DATE(so.order_purchase_timestamp),
                    'yyyyMMdd'
                ) AS INT
            ),
            sc.customer_unique_id,
            so.order_id,
            soi.product_id,
            soi.seller_id,
            so.order_status,
            soi.price,
            so.order_purchase_timestamp,
            so.insert_date
    """
)

fact_sales = fact_sales.withColumn(
    "update_date",
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    )
)

fact_sales.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lh_gold.dbo.fact_sales")

print("The fact_sales table has been created")


# In[6]:


display(
    spark.sql("""
        SELECT
            YEAR(order_purchase_timestamp) AS year,
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_id) AS order_count,
            ROUND(SUM(recognized_revenue), 2) AS revenue
        FROM lh_gold.dbo.fact_sales
        GROUP BY YEAR(order_purchase_timestamp)
        ORDER BY year
    """)
)


# In[3]:


display(
    spark.sql("""
        SELECT
            YEAR(o.order_purchase_timestamp) AS year,
            COUNT(*) AS row_count,
            COUNT(DISTINCT o.order_id) AS order_count
        FROM lh_silver.dbo.silver_orders o
        INNER JOIN lh_silver.dbo.silver_order_items oi
            ON o.order_id = oi.order_id
        GROUP BY YEAR(o.order_purchase_timestamp)
        ORDER BY year
    """)
)


# In[4]:


display(
    spark.sql("""
        SELECT
            YEAR(o.order_purchase_timestamp) AS year,

            COUNT(DISTINCT o.order_id) AS total_orders,

            COUNT(DISTINCT oi.order_id) AS orders_with_items,

            COUNT(DISTINCT c.customer_id) AS matched_customers

        FROM lh_silver.dbo.silver_orders o

        LEFT JOIN lh_silver.dbo.silver_order_items oi
            ON o.order_id = oi.order_id

        LEFT JOIN lh_silver.dbo.silver_customers c
            ON o.customer_id = c.customer_id

        GROUP BY YEAR(o.order_purchase_timestamp)

        ORDER BY year
    """)
)


# In[2]:


display(
    spark.sql(
        """
            select * from fact_sales
            limit 10
        """
    )
)


# In[3]:


display(
    spark.sql(
        """
            SELECT
                YEAR(order_purchase_timestamp) AS year,
                COUNT(*) AS row_count
            FROM lh_gold.dbo.fact_sales
            GROUP BY YEAR(order_purchase_timestamp)
            ORDER BY year
        """
    )
)


# In[4]:


display(
    spark.sql("""
        SELECT 
            order_status,
            COUNT(*) AS row_count
        FROM lh_gold.dbo.fact_sales
        GROUP BY order_status
        ORDER BY row_count DESC
    """)
)


# In[5]:


display(
    spark.sql(
        """
            SELECT *
            FROM lh_gold.dbo.fact_sales fs
            WHERE fs.order_status = 'Canceled'
            LIMIT 3
        """
    )
)

