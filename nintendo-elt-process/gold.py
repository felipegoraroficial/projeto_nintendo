# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.parquet("/mnt/dev/silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS nintendo_databricks.dev.`nintendo-bigtable` (
# MAGIC  titulo STRING, 
# MAGIC  moeda STRING, 
# MAGIC  condition_promo STRING, 
# MAGIC  preco_promo DOUBLE, 
# MAGIC  parcelado STRING, 
# MAGIC  imagem STRING,
# MAGIC  file_name STRING,  
# MAGIC  file_date DATE, 
# MAGIC  memoria STRING, 
# MAGIC  oled STRING, 
# MAGIC  lite STRING, 
# MAGIC  joy_con STRING 
# MAGIC )

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("dev.`nintendo-bigtable`")
