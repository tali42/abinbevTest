# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

import requests
import json
import pytz
import math
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ./nb_parametrizacao

# COMMAND ----------

spark.sql(f"USE {CATALOG_NAME}.{SCHEMA_BRONZE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Utils

# COMMAND ----------

base_url = "https://api.openbrewerydb.org/breweries"
meta_url = f"{base_url}/meta"
page_url = "?page="

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Metadata

# COMMAND ----------

meta_headers = {
    'total': "",
    'page': "",
    'per_page': ""
}

# COMMAND ----------

meta = requests.get(meta_url, headers=meta_headers)
meta = meta.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pagination

# COMMAND ----------

total = meta["total"]
per_page = meta["per_page"]
n_pages = math.ceil(int(total)/int(per_page))

print(f"total rows: {total}")
print(f"rows per page: {per_page}")
print(f"number of pages: {n_pages}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data schema

# COMMAND ----------

data_json_schema = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("brewery_type", StringType()),
    StructField("address_1", StringType()),
    StructField("address_2", StringType()),
    StructField("address_3", StringType()),
    StructField("city", StringType()),
    StructField("state_province", StringType()),
    StructField("postal_code", StringType()),
    StructField("country", StringType()),
    StructField("longitude", StringType()),
    StructField("latitude", StringType()),
    StructField("phone", StringType()),
    StructField("website_url", StringType()),
    StructField("state", StringType()),
    StructField("street", StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Load

# COMMAND ----------

# MAGIC %md
# MAGIC ## api reading function

# COMMAND ----------

def get_data_to_df(n_pages=n_pages):
    page = 1
    df = spark.createDataFrame([], data_json_schema)

    while page <= n_pages:
        print(f"lendo pagina: {page}")
        try:
            response = requests.get(f"{base_url}{page_url}{page}&per_page={per_page}")

            if response.status_code == 200:
                json = response.json()
                df_tmp = spark.createDataFrame(json, schema=data_json_schema)
                df = df.unionByName(df_tmp)

        except Exception as e:
            print(f"Erro na página {page}: {e}")
        
        page += 1
    
    return df

# COMMAND ----------

df_full = get_data_to_df()

# COMMAND ----------

if int((df_full.count())) == int(total):
    print("Todas as linhas foram carregadas com sucesso.")
    print(f"Total de linhas esperadas: {total} \nTotal de linhas carregadas: {df_full.count()}")
else:
    print("Erro ao carregar dados:")
    print(f"Total de linhas esperadas: {total} \nTotal de linhas carregadas: {df_full.count()}")

# COMMAND ----------

df_full.printSchema()

# COMMAND ----------

df_full.columns

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Table

# COMMAND ----------

df_full.createOrReplaceTempView("vw_brewries_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE brewries_bronze
# MAGIC SELECT * FROM vw_brewries_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE brewries_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM brewries_bronze
