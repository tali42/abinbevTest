# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

CATALOG_NAME = "db_brewries"

SCHEMA_BRONZE_NAME = "bronze"
SCHEMA_BRONZE = f"{CATALOG_NAME}.{SCHEMA_BRONZE_NAME}"

SCHEMA_SILVER_NAME = "silver"
SCHEMA_SILVER = f"{CATALOG_NAME}.{SCHEMA_SILVER_NAME}"

SCHEMA_GOLD_NAME = "gold"
SCHEMA_GOLD = f"{CATALOG_NAME}.{SCHEMA_GOLD_NAME}"

LOCATION = "abfss://brewries@extstoragedemo.dfs.core.windows.net/"

SCHEMA_BRONZE_LOCATION = f"{LOCATION}bronze"
SCHEMA_BRONZE_DESCRIPTION = "Schema used for loading raw data from API openbrewerydb"

SCHEMA_SILVER_LOCATION = f"{LOCATION}silver"
SCHEMA_SILVER_DESCRIPTION = "Schema used for brewries transformed data"

SCHEMA_GOLD_LOCATION = f"{LOCATION}gold"
SCHEMA_GOLD_DESCRIPTION = "Schema used for brewries curated/aggregated data"
