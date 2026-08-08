#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import col, from_utc_timestamp, current_timestamp

dim_seller = spark.sql(
    """
        SELECT
            seller_id,
            FIRST(seller_zip_code_prefix) AS seller_zip_code_prefix,
            FIRST(seller_city) AS seller_city,
            FIRST(seller_state) AS seller_state,
            MIN(insert_date) AS insert_date
        FROM lh_silver.dbo.silver_sellers ss
        WHERE ss.seller_id IS NOT NULL
        GROUP BY seller_id
    """
)

dim_seller = dim_seller.withColumn(
    "update_date",
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    )
)

dim_seller.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lh_gold.dbo.dim_seller")

print("The dim_seller table has been created")


# In[2]:


display(
    spark.sql("""
        SELECT *
        FROM lh_gold.dbo.dim_seller
        WHERE seller_id = 'ea6b12bf9ffe2bac34602ec631d97a47'
    """)
)


# In[3]:


display(
    spark.sql("""
        SELECT
            seller_id,
            COUNT(*) AS occurrence_count
        FROM lh_gold.dbo.dim_seller
        GROUP BY seller_id
        HAVING COUNT(*) > 1
    """)
)


# In[4]:


display(
    spark.sql(
        """
            SELECT *
            FROM lh_gold.dbo.dim_seller ds
            WHERE YEAR(ds.update_date) = 2026
        """
    )
)

