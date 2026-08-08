#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql.functions import (
    from_utc_timestamp,
    current_timestamp
)

# ============================================================
# GOLD FACT REVIEW
# Grain: 1 row = 1 review / order
# ============================================================

fact_review = spark.sql(
"""
WITH base_review AS (

    SELECT

        sc.customer_unique_id AS customer_id,

        so.order_id,

        so.order_purchase_timestamp,

        so.order_delivered_customer_date,

        so.insert_date,

        sr.review_id AS source_review_id,

        sr.review_score AS source_review_score,

        sr.review_comment_title AS source_review_comment_title,

        sr.review_comment_message AS source_review_comment_message,

        sr.review_creation_date AS source_review_creation_date,

        sr.review_answer_timestamp AS source_review_answer_timestamp,


        -- ====================================================
        -- Identify synthetic reviews for fake orders 2019-2026
        -- ====================================================

        CASE
            WHEN YEAR(so.order_purchase_timestamp)
                    BETWEEN 2019 AND 2026
                 AND sr.review_id IS NULL
            THEN 1

            ELSE 0
        END AS is_synthetic_review,


        -- ====================================================
        -- REVIEW SCORE
        -- Keep real review score when available.
        -- Generate a deterministic score for fake orders.
        --
        -- Distribution is weighted toward scores 4-5
        -- to resemble the historical Olist review distribution.
        -- ====================================================

        CASE

            WHEN sr.review_score IS NOT NULL
                THEN sr.review_score

            WHEN YEAR(so.order_purchase_timestamp)
                    BETWEEN 2019 AND 2026
            THEN

                CASE

                    WHEN PMOD(HASH(so.order_id), 100) < 5
                        THEN 1

                    WHEN PMOD(HASH(so.order_id), 100) < 12
                        THEN 2

                    WHEN PMOD(HASH(so.order_id), 100) < 25
                        THEN 3

                    WHEN PMOD(HASH(so.order_id), 100) < 55
                        THEN 4

                    ELSE 5

                END

            ELSE 0

        END AS effective_review_score,


        -- ====================================================
        -- REVIEW CREATION DATE
        --
        -- Real reviews:
        --     use original review_creation_date
        --
        -- Synthetic reviews:
        --     delivered date + 1 to 4 days
        -- ====================================================

        CASE

            WHEN sr.review_creation_date IS NOT NULL
                THEN sr.review_creation_date

            WHEN YEAR(so.order_purchase_timestamp)
                    BETWEEN 2019 AND 2026
            THEN

                CAST(
                    DATE_ADD(
                        TO_DATE(
                            so.order_delivered_customer_date
                        ),
                        PMOD(
                            HASH(so.order_id),
                            4
                        ) + 1
                    )
                    AS TIMESTAMP
                )

            ELSE NULL

        END AS effective_review_creation_date


    FROM lh_silver.dbo.silver_orders so


    INNER JOIN lh_silver.dbo.silver_customers sc
        ON so.customer_id = sc.customer_id


    LEFT JOIN lh_silver.dbo.silver_reviews sr
        ON so.order_id = sr.order_id


    WHERE
        so.order_delivered_customer_date IS NOT NULL
),


review_enriched AS (

    SELECT

        *,

        -- ====================================================
        -- REVIEW ANSWER TIMESTAMP
        --
        -- Synthetic response:
        -- review date + 0 to 2 days
        -- ====================================================

        CASE

            WHEN source_review_answer_timestamp IS NOT NULL
                THEN source_review_answer_timestamp

            WHEN is_synthetic_review = 1
            THEN

                CAST(
                    DATE_ADD(
                        TO_DATE(
                            effective_review_creation_date
                        ),
                        PMOD(
                            HASH(
                                CONCAT(
                                    order_id,
                                    '_answer'
                                )
                            ),
                            3
                        )
                    )
                    AS TIMESTAMP
                )

            ELSE NULL

        END AS effective_review_answer_timestamp

    FROM base_review
)


SELECT DISTINCT

    -- ========================================================
    -- DATE KEY
    -- ========================================================

    CAST(
        DATE_FORMAT(
            TO_DATE(
                effective_review_creation_date
            ),
            'yyyyMMdd'
        )
        AS INT
    ) AS date_key,


    -- ========================================================
    -- KEYS
    -- ========================================================

    customer_id,

    order_id,


    -- ========================================================
    -- REVIEW ID
    -- ========================================================

    CASE

        WHEN source_review_id IS NOT NULL
            THEN source_review_id

        WHEN is_synthetic_review = 1
            THEN CONCAT(
                'SYNTH_',
                order_id
            )

        ELSE 'NO_REVIEW'

    END AS review_id,


    -- ========================================================
    -- REVIEW SCORE
    -- ========================================================

    effective_review_score AS review_score,


    -- ========================================================
    -- REVIEW GROUP
    -- ========================================================

    CASE

        WHEN source_review_id IS NULL
             AND is_synthetic_review = 0
            THEN 'Order has no review'

        WHEN effective_review_score >= 4
            THEN 'Positive'

        WHEN effective_review_score = 3
            THEN 'Neutral'

        WHEN effective_review_score <= 2
            THEN 'Negative'

        ELSE 'Unknown'

    END AS review_group,


    -- ========================================================
    -- HAS REVIEW FLAG
    -- ========================================================

    CASE

        WHEN source_review_id IS NOT NULL
             OR is_synthetic_review = 1
            THEN 1

        ELSE 0

    END AS has_review,


    -- ========================================================
    -- REVIEW TITLE
    -- ========================================================

    CASE

        WHEN source_review_comment_title IS NOT NULL
            THEN source_review_comment_title

        WHEN is_synthetic_review = 1
            THEN 'Synthetic customer review'

        ELSE 'Order has no review title'

    END AS review_comment_title,


    -- ========================================================
    -- REVIEW MESSAGE
    -- ========================================================

    CASE

        WHEN source_review_comment_message IS NOT NULL
            THEN source_review_comment_message

        WHEN is_synthetic_review = 1
            THEN 'Synthetic review generated for analytical testing'

        ELSE 'Order has no review content'

    END AS review_comment_message,


    -- ========================================================
    -- REVIEW DATES
    -- ========================================================

    effective_review_creation_date
        AS review_creation_date,

    effective_review_answer_timestamp
        AS review_answer_timestamp,


    -- ========================================================
    -- RESPONSE DAYS
    -- ========================================================

    CASE

        WHEN effective_review_creation_date IS NOT NULL
             AND effective_review_answer_timestamp IS NOT NULL

        THEN DATEDIFF(
            effective_review_answer_timestamp,
            effective_review_creation_date
        )

        ELSE 0

    END AS review_response_days,


    -- ========================================================
    -- RESPONSE DESCRIPTION
    -- ========================================================

    CASE

        WHEN source_review_id IS NULL
             AND is_synthetic_review = 0
            THEN 'No review'

        WHEN effective_review_answer_timestamp IS NULL
            THEN 'No response'

        WHEN DATEDIFF(
            effective_review_answer_timestamp,
            effective_review_creation_date
        ) = 0
            THEN 'Same day'

        WHEN DATEDIFF(
            effective_review_answer_timestamp,
            effective_review_creation_date
        ) = 1
            THEN '1 day'

        ELSE CONCAT(
            DATEDIFF(
                effective_review_answer_timestamp,
                effective_review_creation_date
            ),
            ' days'
        )

    END AS review_response_description,


    insert_date


FROM review_enriched
"""
)


# ============================================================
# UPDATE DATE - NEW YORK TIMEZONE
# ============================================================

fact_review = fact_review.withColumn(
    "update_date",
    from_utc_timestamp(
        current_timestamp(),
        "America/New_York"
    )
)


# ============================================================
# WRITE TO GOLD
# ============================================================

fact_review.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lh_gold.dbo.fact_review")


print(
    "The fact_review table has been created successfully."
)


# ============================================================
# VALIDATION 1 - REVIEW DATA BY YEAR
# ============================================================

display(
    spark.sql("""
        SELECT

            YEAR(review_creation_date) AS year,

            COUNT(*) AS row_count,

            COUNT(DISTINCT order_id)
                AS reviewed_orders,

            ROUND(
                AVG(
                    CASE
                        WHEN has_review = 1
                        THEN review_score
                    END
                ),
                2
            ) AS avg_review_score

        FROM lh_gold.dbo.fact_review

        GROUP BY
            YEAR(review_creation_date)

        ORDER BY
            year
    """)
)


# ============================================================
# VALIDATION 2 - REVIEW DISTRIBUTION
# ============================================================

display(
    spark.sql("""
        SELECT

            YEAR(review_creation_date) AS year,

            review_group,

            COUNT(*) AS review_count

        FROM lh_gold.dbo.fact_review

        WHERE
            review_creation_date IS NOT NULL

        GROUP BY
            YEAR(review_creation_date),
            review_group

        ORDER BY
            year,
            review_group
    """)
)


# In[3]:


display(
    spark.sql("""
        SELECT
            YEAR(review_creation_date) AS year,
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_id) AS reviewed_orders,
            ROUND(AVG(review_score), 2) AS avg_review_score
        FROM lh_gold.dbo.fact_review
        GROUP BY YEAR(review_creation_date)
        ORDER BY year
    """)
)


# In[2]:


display(
    spark.sql(
        """
            SELECT *
            FROM lh_gold.dbo.fact_review
            WHERE has_review = 0
        """
    )
)

