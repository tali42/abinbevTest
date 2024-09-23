# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../nb_parametrizacao

# COMMAND ----------

spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_GOLD_NAME}")

# COMMAND ----------

print(f"schema_location:{SCHEMA_GOLD_LOCATION}")

# COMMAND ----------

create_locations_sql = f"""
CREATE TABLE IF NOT EXISTS locations (
  id_location    STRING       COMMENT 'Location Unique Identifier',
  city           STRING       COMMENT 'City of the Brewry location',
  state          STRING       COMMENT 'State of the Brewry location',
  country        STRING       COMMENT 'Country of the Brewry'
)
USING DELTA
LOCATION '{SCHEMA_GOLD_LOCATION}/locations'
"""

spark.sql(create_locations_sql)

# COMMAND ----------

create_types_sql = f"""
CREATE TABLE IF NOT EXISTS types (
  id_type        STRING       COMMENT 'Brewry Type Unique Identifier',
  brewery_type   STRING       COMMENT 'Type of the Brewry'
)
USING DELTA
LOCATION '{SCHEMA_GOLD_LOCATION}/types'
"""

spark.sql(create_types_sql)

# COMMAND ----------

create_brewries_sql = f"""
CREATE TABLE IF NOT EXISTS brewries (
  id             STRING       COMMENT 'Brewry Unique Identifier',
  name           STRING       COMMENT 'Name of the Brewry',
  address_1      STRING       COMMENT 'First Address Line',
  address_2      STRING       COMMENT 'Second Address Line',
  address_3      STRING       COMMENT 'Third Address Line',
  street         STRING       COMMENT 'Street of the Address',
  postal_code    STRING       COMMENT 'Postal Code of the Brewry location',
  longitude      DECIMAL(9,6) COMMENT 'Longitude Data of the Brewry location',
  latitude       DECIMAL(8,6) COMMENT 'Latitude Data of the Brewry location',
  phone          STRING       COMMENT 'Phone Number of the Brewry contact',
  website_url    STRING       COMMENT 'website url of the Brewry',
  id_location    STRING       COMMENT 'Location Unique Identifier',
  id_type        STRING       COMMENT 'Brewry Type Unique Identifier'
)
USING DELTA
LOCATION '{SCHEMA_GOLD_LOCATION}/brewries'
"""

spark.sql(create_brewries_sql)
