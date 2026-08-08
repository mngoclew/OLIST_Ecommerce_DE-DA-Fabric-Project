#!/usr/bin/env python
# coding: utf-8

# In[1]:


dim_date = spark.sql(
    """
        SELECT
            CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT) AS date_key,
            full_date,
            DAY(full_date) AS day_of_month,
            MONTH(full_date) AS month_number,
            QUARTER(full_date) AS quarter_number,
            YEAR(full_date) AS year_number,
            CONCAT('Month ', MONTH(full_date)) AS month_name,
            DAYOFWEEK(full_date) AS day_of_week_number,

            CASE DAYOFWEEK(full_date)
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday'
                WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
            END AS day_name,

            CASE
                WHEN DAYOFWEEK(full_date) IN (1, 7) THEN 'Weekend'
                ELSE 'Weekday'
            END AS day_type

        FROM (
            SELECT EXPLODE(
                SEQUENCE(
                    TO_DATE('2016-01-01'),
                    TO_DATE('2026-12-31'),
                    INTERVAL 1 DAY
                )
            ) AS full_date
        )
    """
)

dim_date.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dim_date")

print("The dim_date table has been created")

