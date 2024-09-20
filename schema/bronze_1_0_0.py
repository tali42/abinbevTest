# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../nb_parametrizacao

# COMMAND ----------

spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_BRONZE_NAME}")

# COMMAND ----------

print(f"schema_location:{SCHEMA_BRONZE_LOCATION}")

# COMMAND ----------

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS brewries_bronze (
  id             STRING COMMENT '',
  name           STRING COMMENT '',
  brewery_type   STRING COMMENT '',
  address_1      STRING COMMENT '',
  address_2      STRING COMMENT '',
  address_3      STRING COMMENT '',
  city           STRING COMMENT '',
  state_province STRING COMMENT '',
  postal_code    STRING COMMENT '',
  country        STRING COMMENT '',
  longitude      STRING COMMENT '',
  latitude       STRING COMMENT '',
  phone          STRING COMMENT '',
  website_url    STRING COMMENT '',
  state          STRING COMMENT '',
  street         STRING COMMENT ''
) USING DELTA LOCATION '{SCHEMA_BRONZE_LOCATION}/brewries_bronze'
"""

spark.sql(create_table_sql)
