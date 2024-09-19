# Databricks notebook source
import requests
import json
import pytz
import math
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

base_url = "https://api.openbrewerydb.org/breweries"
meta_url = f"{base_url}/meta"
page_url = "?page="

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

total = meta["total"]
per_page = meta["per_page"]
n_pages = math.ceil(int(total)/int(per_page))

# COMMAND ----------

print(f"total rows: {total}")
print(f"rows per page: {per_page}")
print(f"number of pages: {n_pages}")

# COMMAND ----------

data_df = requests.get(base_url)
data_df = data_df.json()

# COMMAND ----------

print(data_df)

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


# df = spark.createDataFrame(data_df, schema=data_json_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # carregamento dos dados full em DataFrame

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

df = get_data_to_df(n_pages=10)

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df_full = get_data_to_df()

# COMMAND ----------

display(df_full)

# COMMAND ----------

df_full.count()

# COMMAND ----------

print(f"total rows: {total}")
print(f"rows per page: {per_page}")
print(f"number of pages: {n_pages}")

# COMMAND ----------


