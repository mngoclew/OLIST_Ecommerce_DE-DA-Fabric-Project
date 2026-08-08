#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import col, from_utc_timestamp, current_timestamp

fact_customer_behavior = spark.sql(
    """
        SELECT
            sc.customer_unique_id AS customer_id,
            sc.customer_zip_code_prefix AS zip_code_prefix,
            sc.customer_city AS city,
            sc.customer_state AS state,

            COUNT(DISTINCT so.order_id) AS total_orders,

            COUNT(DISTINCT CASE
                WHEN so.order_status = 'delivered'
                THEN so.order_id
            END) AS delivered_orders,

            COUNT(DISTINCT CASE
                WHEN so.order_status = 'canceled'
                THEN so.order_id
            END) AS canceled_orders,

            SUM(
                COALESCE(order_value.ordered_product_value, 0)
            ) AS total_ordered_product_value,

            SUM(
                CASE
                    WHEN so.order_status = 'delivered'
                    THEN COALESCE(order_value.ordered_product_value, 0)
                    ELSE 0
                END
            ) AS total_recognized_revenue,

            SUM(
                COALESCE(order_value.total_shipping_cost, 0)
            ) AS total_shipping_cost,

            SUM(
                COALESCE(order_value.product_quantity, 0)
            ) AS total_products_purchased,

            COUNT(DISTINCT sr.review_id) AS review_count,

            AVG(sr.review_score) AS average_review_score,

            SUM(
                CASE
                    WHEN sr.review_score >= 4 THEN 1
                    ELSE 0
                END
            ) AS positive_review_count,

            SUM(
                CASE
                    WHEN sr.review_score = 3 THEN 1
                    ELSE 0
                END
            ) AS neutral_review_count,

            SUM(
                CASE
                    WHEN sr.review_score <= 2 THEN 1
                    ELSE 0
                END
            ) AS negative_review_count,

            MIN(
                TO_DATE(so.order_purchase_timestamp)
            ) AS first_purchase_date,

            MAX(
                TO_DATE(so.order_purchase_timestamp)
            ) AS most_recent_purchase_date,

            CASE
                WHEN COUNT(DISTINCT so.order_id) >= 5
                    THEN 'Loyal customer'
                WHEN COUNT(DISTINCT so.order_id) BETWEEN 2 AND 4
                    THEN 'Returning customer'
                WHEN COUNT(DISTINCT so.order_id) = 1
                    THEN 'One-time customer'
                ELSE 'Unknown'
            END AS purchase_frequency_group,

            CASE
                WHEN SUM(
                    CASE
                        WHEN so.order_status = 'delivered'
                        THEN COALESCE(order_value.ordered_product_value, 0)
                        ELSE 0
                    END
                ) >= 1000
                    THEN 'High-value customer'

                WHEN SUM(
                    CASE
                        WHEN so.order_status = 'delivered'
                        THEN COALESCE(order_value.ordered_product_value, 0)
                        ELSE 0
                    END
                ) >= 500
                    THEN 'Medium-value customer'

                ELSE 'Low-value customer'
            END AS customer_value_group,

            CASE
                WHEN COUNT(DISTINCT so.order_id) >= 2 THEN 1
                ELSE 0
            END AS is_returning_customer,

            CASE
                WHEN COUNT(DISTINCT CASE
                    WHEN so.order_status = 'canceled'
                    THEN so.order_id
                END) > 0 THEN 1
                ELSE 0
            END AS has_canceled_order,

            so.insert_date AS insert_date

        FROM lh_silver.dbo.silver_customers sc

        LEFT JOIN lh_silver.dbo.silver_orders so
            ON sc.customer_id = so.customer_id

        LEFT JOIN (
            SELECT
                order_id,
                SUM(price) AS ordered_product_value,
                SUM(freight_value) AS total_shipping_cost,
                COUNT(order_item_id) AS product_quantity
            FROM lh_silver.dbo.silver_order_items
            GROUP BY order_id
        ) order_value
            ON so.order_id = order_value.order_id

        LEFT JOIN lh_silver.dbo.silver_reviews sr
            ON so.order_id = sr.order_id

        GROUP BY
            sc.customer_unique_id,
            sc.customer_zip_code_prefix,
            sc.customer_city,
            sc.customer_state,
            so.insert_date
    """
)

fact_customer_behavior = fact_customer_behavior.withColumn(
    "update_date",
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    )
)

fact_customer_behavior.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_customer_behavior")

print("The fact_customer_behavior table has been created")


# In[2]:


display(
    spark.sql(
        """
            SELECT *
            FROM lh_gold.dbo.fact_customer_behavior
            WHERE total_orders >= 3
            LIMIT 10
        """
    )
)

