# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ./nb_parametrizacao

# COMMAND ----------

# MAGIC %md
# MAGIC # herdando camada bronze para tratamento

# COMMAND ----------

spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_BRONZE_NAME}")

# COMMAND ----------

brewries_bronze = spark.table("brewries_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC # Tratamento de Dados para Silver

# COMMAND ----------

spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_SILVER_NAME}")

# COMMAND ----------

display(brewries_bronze)

# COMMAND ----------

df = brewries_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing unique identifier

# COMMAND ----------

df.count()

# COMMAND ----------

df.distinct().count()

# COMMAND ----------

df.select('id').distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## String Columns

# COMMAND ----------

brewries_str_columns = ['id', 'name', 'brewery_type', 'address_1', 'address_2', 'address_3', 'city', 'state_province', 'postal_code', 'country', 'website_url', 'state', 'street', 'phone']

# COMMAND ----------

for column in brewries_str_columns:
    df = df.withColumn(column, trim(col(column)))

    if ( brewries_bronze.where(col(column)=="").count() > 0):
        print(f"\nThere is empty str values in column: {column}")
        df = df.withColumn(column, when(col(column)=="", lit(None)).otherwise(col(column)))
        print(f"\nEmpty str values in column: {column} Treated!")

    if ( brewries_bronze.where(col(column)==" ").count() > 0):
        print(f"\nThere is space str values in column: {column}")
        df = df.withColumn(column, when(col(column)==" ", lit(None)).otherwise(col(column)))
        print(f"\nEmpty space str in column: {column} Treated!")

    if ( brewries_bronze.where(col(column)=="null").count() > 0):
        print(f"\nThere is null str values in column: {column}")
        df = df.withColumn(column, when(col(column)=="null", lit(None)).otherwise(col(column)))
        print(f"\nEmpty null str in column: {column} Treated!")

# COMMAND ----------

# MAGIC %md
# MAGIC ~~Int Column~~

# COMMAND ----------

display(df.select("phone").distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decimals

# COMMAND ----------

display(df.select("latitude").distinct())

# COMMAND ----------

display(df.select("longitude").distinct())

# COMMAND ----------

df = (df
      .withColumn("latitude", col("latitude").cast("Decimal(8,6)"))
      .withColumn("longitude", col("longitude").cast("Decimal(9,6)"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving Silver table partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ## exploring possible partitions

# COMMAND ----------

df.count()

# COMMAND ----------

display(df.groupBy("state").agg(count("id").alias("brewries")))

# COMMAND ----------

display(df.groupBy("country").agg(count("id").alias("brewries")))

# COMMAND ----------

display(df.select("state", "state_province").where(col("state")!=col("state_province")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## INSERT INTO

# COMMAND ----------

columns = ['id',
'name',
'brewery_type',
'address_1',
'address_2',
'address_3',
'street',
'city',
'state',
'postal_code',
'country',
'longitude',
'latitude',
'phone',
'website_url']

# COMMAND ----------

df.select(columns) \
  .write \
  .mode("overwrite") \
  .insertInto("brewries_silver")
