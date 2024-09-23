# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../nb_parametrizacao

# COMMAND ----------

spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_SILVER_NAME}")

# COMMAND ----------

print(f"schema_location:{SCHEMA_SILVER_LOCATION}")

# COMMAND ----------

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS brewries_silver (
  id             STRING       COMMENT 'Brewry Unique Identifier',
  name           STRING       COMMENT 'Name of the Brewry',
  brewery_type   STRING       COMMENT 'Type of the Brewry',
  address_1      STRING       COMMENT 'First Address Line',
  address_2      STRING       COMMENT 'Second Address Line',
  address_3      STRING       COMMENT 'Third Address Line',
  street         STRING       COMMENT 'Street of the Address',
  city           STRING       COMMENT 'City of the Brewry location',
  state          STRING       COMMENT 'State of the Brewry location',
  postal_code    STRING       COMMENT 'Postal Code of the Brewry location',
  country        STRING       COMMENT 'country of the Brewry',
  longitude      DECIMAL(9,6) COMMENT 'Longitude Data of the Brewry location',
  latitude       DECIMAL(8,6) COMMENT 'Latitude Data of the Brewry location',
  phone          STRING       COMMENT 'Phone Number of the Brewry contact',
  website_url    STRING       COMMENT 'website url of the Brewry'
)
USING DELTA
PARTITIONED BY(country, state, city)
LOCATION '{SCHEMA_SILVER_LOCATION}/brewries_silver'
"""

spark.sql(create_table_sql)
