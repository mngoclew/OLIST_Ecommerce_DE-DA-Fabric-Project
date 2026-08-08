#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import Row
from pyspark.sql.functions import from_utc_timestamp, current_timestamp

pipeline_name = "pipeline_dim_seller"
activity_name = "dim_seller"
table_name = "dim_seller"
status = "FAILED"
error_message = "dim_seller failed while running the pipeline"

log_data = [Row(
    pipeline_name=pipeline_name,
    activity_name=activity_name,
    table_name=table_name,
    status=status,
    error_message=error_message
)]

df_log = spark.createDataFrame(log_data)

df_log = df_log.withColumn(
    "log_time",
    from_utc_timestamp(current_timestamp(), "America/New_York")
)

df_log.write \
    .mode("append") \
    .format("delta") \
    .saveAsTable("log_pipeline")

print("The error log has been written to the log_pipeline table")


# In[ ]:


display(
    spark.sql(
        """
            select * from log_pipeline
        """
    )
)


# In[ ]:


from pyspark.sql.functions import current_timestamp, from_utc_timestamp

display(
    spark.sql("""
        SELECT
            current_timestamp() AS spark_current_time,
            from_utc_timestamp(
                current_timestamp(),
                'America/New_York'
            ) AS new_york_time
    """)
)


# In[ ]:


from notebookutils import mssparkutils

print(dir(mssparkutils.notebook))


# In[ ]:


from notebookutils import mssparkutils

print(dir(mssparkutils.env))


# In[ ]:


display(
    spark.sql("""
        SELECT *
        FROM log_pipeline
        ORDER BY log_time DESC
    """)
)

