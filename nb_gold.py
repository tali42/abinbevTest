# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ./nb_parametrizacao

# COMMAND ----------

# MAGIC %md
# MAGIC # herdando camada silver

# COMMAND ----------

silver = spark.table("db_brewries.silver.brewries_silver")

# COMMAND ----------

df = silver

# COMMAND ----------

display(df)

# COMMAND ----------

spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_GOLD_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC # creating id_hashs

# COMMAND ----------

def create_hashed_column(df, col_name,*cols):
  cols_concat = concat_ws("||", *[col(c) for c in cols])
  return df.withColumn(col_name, md5(cols_concat))

# COMMAND ----------

display(df.select("city", "state", "country").distinct())

# COMMAND ----------

display(df.select("brewery_type").distinct())

# COMMAND ----------

locations_columns = ["city", "state", "country"]
df = create_hashed_column(df, "id_location", *locations_columns)

# COMMAND ----------

type_columns = ["brewery_type"]
df = create_hashed_column(df, "id_type", *type_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Gold Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Locations Table

# COMMAND ----------

df_locations = df.select("id_location", "city", "state", "country").distinct()

# COMMAND ----------

df_locations.write \
  .mode("overwrite") \
  .insertInto("locations")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Table

# COMMAND ----------

df_type = df.select("id_type", "brewery_type").distinct()

# COMMAND ----------

df_type.write \
  .mode("overwrite") \
  .insertInto("types")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Brewries Table

# COMMAND ----------

df = df.drop(*locations_columns) \
  .drop(*type_columns)

# COMMAND ----------

df.write \
  .mode("overwrite") \
  .insertInto("brewries")

# COMMAND ----------

# MAGIC %md
# MAGIC # Agg View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW brewries_by_type_and_location AS
# MAGIC
# MAGIC SELECT l.city, l.state, l.country, t.brewery_type, COUNT(DISTINCT b.id) AS brewries
# MAGIC FROM db_brewries.gold.brewries b
# MAGIC   JOIN db_brewries.gold.locations l ON b.id_location = l.id_location
# MAGIC   JOIN db_brewries.gold.types t ON b.id_type = t.id_type
# MAGIC GROUP BY l.city, l.state, l.country, t.brewery_type;
