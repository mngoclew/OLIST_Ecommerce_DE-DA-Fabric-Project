#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import col, from_utc_timestamp, current_timestamp

fact_payment = spark.sql(
    """
        SELECT
            CAST(
                DATE_FORMAT(
                    TO_DATE(so.order_purchase_timestamp),
                    'yyyyMMdd'
                ) AS INT
            ) AS date_key,

            sp.order_id,
            sc.customer_unique_id AS customer_id,

            CASE sp.payment_type
                WHEN 'credit_card' THEN 'Credit card'
                WHEN 'debit_card' THEN 'Debit card'
                WHEN 'not_defined' THEN 'Payment method not recorded'
                WHEN 'voucher' THEN 'Voucher'
                WHEN 'boleto' THEN 'Payment slip'
                ELSE 'Other'
            END AS payment_type,

            sp.payment_sequential,

            CONCAT(
                'Payment number ',
                sp.payment_sequential
            ) AS payment_sequence_description,

            sp.payment_installments,

            CASE
                WHEN sp.payment_installments = 0 THEN 'Unknown'
                WHEN sp.payment_installments = 1 THEN 'Single payment'
                WHEN sp.payment_installments BETWEEN 2 AND 3
                    THEN 'Short-term installments'
                WHEN sp.payment_installments BETWEEN 4 AND 6
                    THEN 'Medium-term installments'
                WHEN sp.payment_installments BETWEEN 7 AND 12
                    THEN 'Long-term installments'
                WHEN sp.payment_installments BETWEEN 13 AND 24
                    THEN 'Very long-term installments'
                ELSE 'Other'
            END AS installment_group,

            sp.payment_value AS payment_amount,
            so.order_purchase_timestamp,

            so.insert_date AS insert_date

        FROM lh_silver.dbo.silver_orders so

        JOIN lh_silver.dbo.silver_payments sp
            ON so.order_id = sp.order_id

        JOIN lh_silver.dbo.silver_customers sc
            ON so.customer_id = sc.customer_id
    """
)

fact_payment = fact_payment.withColumn(
    "update_date",
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    )
)

fact_payment.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_payment")

print("The fact_payment table has been created")

