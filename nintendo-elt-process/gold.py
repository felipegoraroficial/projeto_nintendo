# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo desse notebook
# MAGIC
# MAGIC O objetivo deste notebook é carregar os dados do projeto Nintendo em uma extarnal table onde apenas será registrado novos registro a tabela. 
# MAGIC
# MAGIC 1 - faz leitura do arquivo json config para obter o env em questão.
# MAGIC
# MAGIC 2 - importa funções do repositório meus_scripts_pyspark.
# MAGIC
# MAGIC 3 - Executa uma função sql que cria a external table com a location delta table e partition by file_date, caso a tabela não exista.
# MAGIC
# MAGIC 4 - chama a função que identifica novos registro, caso a tabela não exista, o dataframe silver é inserido a external table.
# MAGIC
# MAGIC 5 - carrega o dataframe de novos registro na external table

# COMMAND ----------

import os
import json
import sys

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
storage = config["storage"]

# COMMAND ----------

# Define o caminho do diretório Silver no Azure Data Lake Storage
silver_path = f'/Volumes/nintendo_databricks/{env}/silver-vol'

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
    link STRING,  
    file_date DATE,
    status STRING,
    origem STRING, 
    memoria STRING, 
    oled STRING, 
    lite STRING
)
USING DELTA
LOCATION 'abfss://{env}@{storage}.dfs.core.windows.net/gold/'
PARTITIONED BY (file_date)
"""

# Executa a consulta SQL para criar a tabela externa
spark.sql(query)

# COMMAND ----------

# Adiciona o caminho do diretório 'meus_scripts_pyspark' ao sys.path
# Isso permite que módulos Python localizados nesse diretório sejam importados
# Diretorio referente a funções de pyspark
sys.path.append(f'{current_dir}/meus_scripts_pyspark')

# COMMAND ----------

from verify_new_lines import verify_new_lines

# COMMAND ----------

# Carregando tabela gold antiga
try:
    tabela = spark.read.table(f"nintendo_databricks.{env}.`nintendo-bigtable`")
except:
    pass

# Verifica se a tabela está vazia
if tabela.rdd.isEmpty():
    
    novos_registros = df

else:
    # Realize uma junção à esquerda (left anti join) para encontrar os novos registros
    condicao_join = (
        (df["link"] == tabela["link"]) &
        (df["file_date"] != tabela["file_date"])
    )
    novos_registros = verify_new_lines(df, tabela, condicao_join)

#filtra apenas registros ativos
novos_registros = novos_registros.filter(novos_registros['status'] == 'ativo')

# COMMAND ----------

# Escreve o DataFrame no formato Delta no diretório Gold no Azure Data Lake Storage
# Sobrescreve os dados existentes e o esquema, se necessário
# Particiona os dados pela coluna "file_date"
novos_registros.write \
  .format("delta") \
  .mode("append") \
  .option("overwriteSchema", "true") \
  .partitionBy("file_date") \
  .save(f"abfss://{env}@{storage}.dfs.core.windows.net/gold")
