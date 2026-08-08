#!/usr/bin/env python
# coding: utf-8

# In[1]:


spark.sql("DROP TABLE IF EXISTS log_pipeline")

spark.sql("""
CREATE TABLE log_pipeline
(
    pipeline_name STRING,
    activity_name STRING,
    table_name STRING,
    status STRING,
    error_message STRING,
    log_time TIMESTAMP
)
USING DELTA
""")

