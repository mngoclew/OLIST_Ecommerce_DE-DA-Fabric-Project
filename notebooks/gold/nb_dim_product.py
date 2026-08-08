#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import col, from_utc_timestamp, current_timestamp

dim_product = spark.sql(
    """
        SELECT
            sp.product_id,

            CONCAT('Product ', sp.product_id) AS product_name,

            COALESCE(
                FIRST(sp.product_category_name),
                'Uncategorized product'
            ) AS product_category_name,

            COALESCE(
                FIRST(sp.product_name_lenght),
                0
            ) AS product_name_length,

            COALESCE(
                FIRST(sp.product_description_lenght),
                0
            ) AS product_description_length,

            COALESCE(
                FIRST(sp.product_weight_g),
                0
            ) AS product_weight_g,

            COALESCE(
                FIRST(sp.product_length_cm),
                0
            ) AS product_length_cm,

            COALESCE(
                FIRST(sp.product_width_cm),
                0
            ) AS product_width_cm,

            COALESCE(
                FIRST(sp.product_height_cm),
                0
            ) AS product_height_cm,

            (
                COALESCE(FIRST(sp.product_length_cm), 0)
                * COALESCE(FIRST(sp.product_width_cm), 0)
                * COALESCE(FIRST(sp.product_height_cm), 0)
            ) AS volume_cm3,

            CASE
                WHEN FIRST(sp.product_weight_g) IS NULL
                    THEN 'Unknown'
                WHEN FIRST(sp.product_weight_g) < 500
                    THEN 'Light'
                WHEN FIRST(sp.product_weight_g) BETWEEN 500 AND 2000
                    THEN 'Medium'
                WHEN FIRST(sp.product_weight_g) BETWEEN 2001 AND 10000
                    THEN 'Heavy'
                ELSE 'Very heavy'
            END AS weight_group,

            MIN(sp.insert_date) AS insert_date

        FROM lh_silver.dbo.silver_products sp
        WHERE sp.product_id IS NOT NULL
        GROUP BY sp.product_id
    """
)

dim_product = dim_product.withColumn(
    "update_date",
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    )
)

dim_product.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_product")

print("The dim_product table has been created")


# In[2]:


display(
    spark.sql(
        """
            SELECT *
            FROM lh_gold.dbo.dim_product
            -- WHERE product_id = '00066f42aeeb9f3007548bb9d3f33c38'
            LIMIT 10
        """
    )
)

