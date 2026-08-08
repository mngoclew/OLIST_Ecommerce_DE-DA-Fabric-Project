#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import col, from_utc_timestamp, current_timestamp

dim_customer = spark.sql(
    """
        SELECT
            customer_unique_id AS customer_id,
            MAX(customer_zip_code_prefix) AS zip_code_prefix,
            MAX(customer_city) AS city,
            MAX(customer_state) AS state,
            MIN(insert_date) AS insert_date
        FROM lh_silver.dbo.silver_customers
        WHERE customer_unique_id IS NOT NULL
        GROUP BY customer_unique_id
    """
)

dim_customer = dim_customer.withColumn(
    "update_date",
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    )
)

dim_customer.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lh_gold.dbo.dim_customer")

print("The dim_customer table has been created")


# In[2]:


display(
    spark.sql("""
        SELECT COUNT(*) AS total_customers
        FROM lh_gold.dbo.dim_customer
    """)
)


# In[2]:


display(
    spark.sql(
        """
            select * 
            from dim_customer
            limit 10

        """
    )
)

