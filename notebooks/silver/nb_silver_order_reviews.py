#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql.functions import (
    col,
    trim,
    to_timestamp,
    from_utc_timestamp,
    current_timestamp
)

# Source: lh_olist_bronze / Files / olist_order_reviews_dataset.csv

# Destination: lh_olist_silver / Tables / silver_reviews

# Grain: 1 row = 1 review for 1 order
# One order may have more than one review
reviews_df = spark.read.csv(
    "abfss://569508d4-c428-48db-8404-4f275c6a813e@onelake.dfs.fabric.microsoft.com/70807d05-5771-492e-8f2f-614ed0f44a9a/Files/olist_order_reviews_dataset_updated",
    header=True,
    inferSchema=True
)

print("Original table row count: ", reviews_df.count())

reviews_df = reviews_df.dropDuplicates()

print("Row count after removing duplicates: ", reviews_df.count())

silver_reviews = reviews_df.select(
    # Review ID
    col("review_id").alias("review_id"),

    # Order ID
    col("order_id").alias("order_id"),

    # Review score for the order
    col("review_score").cast("int").alias("review_score"),

    # Review title
    trim(col("review_comment_title")).alias("review_comment_title"),

    # Review content
    trim(col("review_comment_message")).alias("review_comment_message"),

    # Review creation timestamp
    to_timestamp(
        col("review_creation_date")
    ).alias("review_creation_date"),

    # Review response timestamp
    to_timestamp(
        col("review_answer_timestamp")
    ).alias("review_answer_timestamp"),

    col("insert_date").cast("timestamp").alias("insert_date"),

    # Always update based on the ETL execution time
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    ).alias("update_date")
)

silver_reviews.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_reviews")

print("The silver_reviews table has been created successfully")


# In[1]:


display(
    spark.sql("""
        SELECT
            YEAR(o.order_purchase_timestamp) AS year,
            COUNT(DISTINCT o.order_id) AS total_orders,
            COUNT(DISTINCT r.order_id) AS orders_with_reviews
        FROM lh_silver.dbo.silver_orders o

        LEFT JOIN lh_silver.dbo.silver_reviews r
            ON o.order_id = r.order_id

        GROUP BY YEAR(o.order_purchase_timestamp)
        ORDER BY year
    """)
)


# In[2]:


display(
    spark.sql(
        """
        select *
        from silver_reviews sr
        where sr.order_id = '84aa61a900410cfe26b57337d376a1ae'
        """
    )
)

