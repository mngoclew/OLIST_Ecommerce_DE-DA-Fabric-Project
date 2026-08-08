#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql.functions import from_utc_timestamp, current_timestamp

dim_location = spark.sql(
    """
    SELECT
        sg.geolocation_zip_code_prefix AS zip_code_prefix,
        MIN(sg.geolocation_city) AS city,
        MIN(sg.geolocation_state) AS state,
        AVG(sg.geolocation_lat) AS latitude,
        AVG(sg.geolocation_lng) AS longitude,
        MIN(sg.insert_date) AS insert_date
    FROM lh_silver.dbo.silver_geolocation AS sg
    WHERE sg.geolocation_zip_code_prefix IS NOT NULL
    GROUP BY sg.geolocation_zip_code_prefix
    """
)

dim_location = dim_location.withColumn(
    "update_date",
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    )
)

dim_location.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lh_gold.dbo.dim_location")

print("The dim_location table has been created successfully.")


# In[3]:


display(

    spark.sql(
        """
            select *
            from dim_location 
            Limit 10
        """
    )
)

