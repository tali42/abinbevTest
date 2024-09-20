# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../nb_parametrizacao

# COMMAND ----------

# MAGIC %md
# MAGIC # Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG db_brewries;

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS brewries_bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS brewries_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS brewries_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Tables Versionings

# COMMAND ----------

# MAGIC %run ./bronze_1_0_0

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Tables Versionings

# COMMAND ----------

# MAGIC %run ./silver_1_0_0

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Tables Versionings

# COMMAND ----------

# MAGIC %run ./gold_1_0_0
