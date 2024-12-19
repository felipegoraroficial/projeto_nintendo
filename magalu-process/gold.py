# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.parquet("/mnt/dev/magalu/silver")

# COMMAND ----------

df = df.filter(col("titulo").rlike("(?i)^console"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.`mkt-magalu` (
# MAGIC  titulo STRING, 
# MAGIC  moeda STRING, 
# MAGIC  condition_promo STRING, 
# MAGIC  preco_promo FLOAT, 
# MAGIC  parcelado FLOAT, 
# MAGIC  imagem STRING, 
# MAGIC  file_date DATE, 
# MAGIC  memoria STRING, 
# MAGIC  oled STRING, 
# MAGIC  lite STRING, 
# MAGIC  joy_con STRING 
# MAGIC )

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("dev.`mkt-magalu`")
