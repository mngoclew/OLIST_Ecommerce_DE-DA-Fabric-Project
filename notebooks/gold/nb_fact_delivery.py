#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import from_utc_timestamp, current_timestamp

fact_delivery = spark.sql(
    """
    SELECT
        CAST(
            DATE_FORMAT(TO_DATE(so.order_purchase_timestamp), 'yyyyMMdd')
            AS INT
        ) AS order_date_key,

        CAST(
            DATE_FORMAT(TO_DATE(so.order_approved_at), 'yyyyMMdd')
            AS INT
        ) AS approved_date_key,

        CAST(
            DATE_FORMAT(TO_DATE(so.order_delivered_carrier_date), 'yyyyMMdd')
            AS INT
        ) AS shipped_date_key,

        CAST(
            DATE_FORMAT(TO_DATE(so.order_delivered_customer_date), 'yyyyMMdd')
            AS INT
        ) AS delivered_date_key,

        CAST(
            DATE_FORMAT(TO_DATE(so.order_estimated_delivery_date), 'yyyyMMdd')
            AS INT
        ) AS estimated_date_key,

        so.order_id,
        sc.customer_unique_id AS customer_id,
        sc.customer_state AS state,

        CAST(so.order_purchase_timestamp AS DATE) AS order_date,
        CAST(so.order_approved_at AS DATE) AS approved_date,
        CAST(so.order_delivered_carrier_date AS DATE) AS shipped_date,
        CAST(so.order_delivered_customer_date AS DATE) AS delivered_date,
        CAST(so.order_estimated_delivery_date AS DATE) AS estimated_delivery_date,

        DATEDIFF(
            so.order_delivered_customer_date,
            so.order_purchase_timestamp
        ) AS order_to_delivery_days,

        DATEDIFF(
            so.order_approved_at,
            so.order_purchase_timestamp
        ) AS order_approval_days,

        DATEDIFF(
            so.order_delivered_carrier_date,
            so.order_approved_at
        ) AS order_preparation_days,

        DATEDIFF(
            so.order_delivered_customer_date,
            so.order_delivered_carrier_date
        ) AS shipping_days,

        DATEDIFF(
            so.order_estimated_delivery_date,
            so.order_delivered_customer_date
        ) AS delivery_days_difference,

        CASE
            WHEN DATEDIFF(
                so.order_estimated_delivery_date,
                so.order_delivered_customer_date
            ) > 0 THEN 'Delivered early'

            WHEN DATEDIFF(
                so.order_estimated_delivery_date,
                so.order_delivered_customer_date
            ) = 0 THEN 'Delivered on time'

            ELSE 'Delivered late'
        END AS delivery_status,

        CASE
            WHEN DATEDIFF(
                so.order_delivered_customer_date,
                so.order_purchase_timestamp
            ) <= 3 THEN '<= 3'

            WHEN DATEDIFF(
                so.order_delivered_customer_date,
                so.order_purchase_timestamp
            ) <= 7 THEN '4 - 7'

            WHEN DATEDIFF(
                so.order_delivered_customer_date,
                so.order_purchase_timestamp
            ) <= 10 THEN '8 - 10'

            WHEN DATEDIFF(
                so.order_delivered_customer_date,
                so.order_purchase_timestamp
            ) <= 15 THEN '11 - 15'

            WHEN DATEDIFF(
                so.order_delivered_customer_date,
                so.order_purchase_timestamp
            ) <= 20 THEN '16 - 20'

            WHEN DATEDIFF(
                so.order_delivered_customer_date,
                so.order_purchase_timestamp
            ) <= 30 THEN '21 - 30'

            WHEN DATEDIFF(
                so.order_delivered_customer_date,
                so.order_purchase_timestamp
            ) <= 40 THEN '31 - 40'

            ELSE '> 40'
        END AS delivery_time_group,

        so.insert_date

    FROM lh_silver.dbo.silver_orders so

    JOIN lh_silver.dbo.silver_customers sc
        ON so.customer_id = sc.customer_id

    WHERE so.order_delivered_customer_date IS NOT NULL
    """
)

fact_delivery = fact_delivery.withColumn(
    "update_date",
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    )
)

fact_delivery.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_delivery")

print("The fact_delivery table has been created")


# In[2]:


display(
    spark.sql("""
        SELECT
            fd.state,
            COUNT(fd.order_id) AS order_count
        FROM lh_gold.dbo.fact_delivery fd
        WHERE fd.state = 'SP'
        GROUP BY fd.state
    """)
)


# In[3]:


display(
    spark.sql("""
        SELECT COUNT(fd.order_id) AS order_count
        FROM lh_gold.dbo.fact_delivery fd
    """)
)


# In[6]:


display(
    spark.sql("""
        SELECT
            dc.state,
            COUNT(DISTINCT fd.order_id) AS total_delivered_orders,

            COUNT(DISTINCT CASE
                WHEN fd.delivery_status = 'Delivered on time'
                THEN fd.order_id
            END) AS on_time_delivered_orders,

            ROUND(
                COUNT(DISTINCT CASE
                    WHEN fd.delivery_status = 'Delivered on time'
                    THEN fd.order_id
                END) * 100.0
                / COUNT(DISTINCT fd.order_id),
                2
            ) AS on_time_delivery_rate

        FROM lh_gold.dbo.fact_delivery fd

        LEFT JOIN lh_gold.dbo.dim_customer dc
            ON fd.customer_id = dc.customer_id

        GROUP BY dc.state
        ORDER BY total_delivered_orders DESC
    """)
)


# In[5]:


display(
    spark.sql("""
        SELECT
            order_id,
            COUNT(*) AS fact_row_count
        FROM lh_gold.dbo.fact_delivery
        GROUP BY order_id
        HAVING COUNT(*) >= 2
        ORDER BY fact_row_count DESC
    """)
)

