# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo desse notebook
# MAGIC
# MAGIC O objetivo deste notebook é carregar e processar dados do projeto Nintendo, 
# MAGIC utilizando as configurações especificadas no arquivo config.json. 
# MAGIC A partir dessas configurações, o notebook ajusta o ambiente de execução 
# MAGIC e realiza o processamento dos dados na external location;
# MAGIC É criado uma external table se ela não existir atualmente no catalog e posteriormente os dados são carregados na external table particionando pela coluna file_date

# COMMAND ----------

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

# Define o caminho do diretório Silver no Azure Data Lake Storage
silver_path = f'abfss://{env}@nintendostorageaccount.dfs.core.windows.net/silver/'

# Lê os dados do diretório Silver no formato Parquet e carrega em um DataFrame Spark
df = spark.read.parquet(silver_path)

# COMMAND ----------

# Cria uma consulta SQL para criar uma tabela externa no Delta Lake
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

# Executa a consulta SQL para criar a tabela externa
spark.sql(query)

# COMMAND ----------

# Escreve o DataFrame no formato Delta no diretório Gold no Azure Data Lake Storage
# Sobrescreve os dados existentes e o esquema, se necessário
# Particiona os dados pela coluna "file_date"
df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .partitionBy("file_date") \
  .save(f"abfss://{env}@nintendostorageaccount.dfs.core.windows.net/gold")
