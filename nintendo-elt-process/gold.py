# Databricks notebook source
import os
import json

# COMMAND ----------

# Obtém o caminho do diretório atual
current_dir = os.getcwd()

# Ajusta o caminho do diretório para os primeiros 4 níveis
current_dir = '/'.join(current_dir.split('/')[:4])

# Define o caminho do arquivo de configuração
config_path = f"{current_dir}/projeto_nintendo/config.json"

# Abre o arquivo de configuração e carrega seu conteúdo em um dicionário
with open(config_path, "r") as f:
    config = json.load(f)

# Obtém o valor da chave "env" do dicionário de configuração
env = config["env"]

# COMMAND ----------

silver_path = f'abfss://{env}@nintendostorageaccount.dfs.core.windows.net/silver/'

df = spark.read.parquet(silver_path)

# COMMAND ----------

query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {env}.`nintendo-bigtable` (
    titulo STRING, 
    moeda STRING, 
    condition_promo STRING, 
    preco_promo DOUBLE, 
    parcelado STRING, 
    imagem STRING,
    file_name STRING,  
    file_date DATE, 
    memoria STRING, 
    oled STRING, 
    lite STRING, 
    joy_con STRING 
)
USING DELTA
LOCATION 'abfss://{env}@nintendostorageaccount.dfs.core.windows.net/gold/'
"""
spark.sql(query)

# COMMAND ----------

df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .partitionBy("file_date") \
  .save(f"abfss://{env}@nintendostorageaccount.dfs.core.windows.net/gold")
