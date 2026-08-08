#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql.functions import col, from_utc_timestamp, current_timestamp

fact_seller_delivery = spark.sql(
    """
        SELECT DISTINCT
            CAST(DATE_FORMAT(TO_DATE(so.order_purchase_timestamp), 'yyyyMMdd') AS INT) AS order_date_key,
            CAST(DATE_FORMAT(TO_DATE(so.order_approved_at), 'yyyyMMdd') AS INT) AS approved_date_key,
            CAST(DATE_FORMAT(TO_DATE(soi.shipping_limit_date), 'yyyyMMdd') AS INT) AS seller_limit_date_key,
            CAST(DATE_FORMAT(TO_DATE(so.order_delivered_carrier_date), 'yyyyMMdd') AS INT) AS shipped_date_key,
            CAST(DATE_FORMAT(TO_DATE(so.order_delivered_customer_date), 'yyyyMMdd') AS INT) AS delivered_date_key,

            so.order_id,
            sc.customer_unique_id AS customer_id,
            soi.seller_id,

            ss.seller_city,
            ss.seller_state,

            CAST(so.order_purchase_timestamp AS DATE) AS order_date,
            CAST(so.order_approved_at AS DATE) AS approved_date,
            CAST(soi.shipping_limit_date AS DATE) AS seller_shipping_limit_date,
            CAST(so.order_delivered_carrier_date AS DATE) AS shipped_date,

            DATEDIFF(
                so.order_delivered_carrier_date,
                so.order_approved_at
            ) AS seller_preparation_days,

            DATEDIFF(
                soi.shipping_limit_date,
                so.order_delivered_carrier_date
            ) AS seller_handover_days_difference,

            CASE
                WHEN so.order_delivered_carrier_date IS NULL
                    THEN 'Not handed over'
                WHEN DATEDIFF(
                    soi.shipping_limit_date,
                    so.order_delivered_carrier_date
                ) > 0
                    THEN 'Handed over early'
                WHEN DATEDIFF(
                    soi.shipping_limit_date,
                    so.order_delivered_carrier_date
                ) = 0
                    THEN 'Handed over on time'
                ELSE 'Handed over late'
            END AS seller_handover_status,

            so.insert_date AS insert_date

        FROM lh_silver.dbo.silver_orders so

        JOIN lh_silver.dbo.silver_customers sc
            ON so.customer_id = sc.customer_id

        JOIN lh_silver.dbo.silver_order_items soi
            ON so.order_id = soi.order_id

        JOIN lh_silver.dbo.silver_sellers ss
            ON soi.seller_id = ss.seller_id

        WHERE so.order_delivered_customer_date IS NOT NULL
    """
)

fact_seller_delivery = fact_seller_delivery.withColumn(
    "update_date",
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    )
)

fact_seller_delivery.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_seller_delivery")

print("The fact_seller_delivery table has been created")


# In[3]:


display(
    spark.sql(
        """
        SELECT *
        FROM fact_seller_delivery
        WHERE order_id IN (
            SELECT order_id
            FROM fact_seller_delivery
            GROUP BY order_id
            HAVING COUNT(DISTINCT seller_id) >= 2
        )
        ORDER BY order_id, seller_id
        LIMIT 100
        """
    )
)


# In[4]:


display(
    spark.sql(
        """
            SELECT
                order_id,
                seller_id,
                COUNT(*) AS row_count
            FROM fact_seller_delivery
            GROUP BY
                order_id,
                seller_id
            HAVING COUNT(*) > 1
        """
    )
)

