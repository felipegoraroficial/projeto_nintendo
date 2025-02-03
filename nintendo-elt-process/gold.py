# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo desse notebook
# MAGIC
# MAGIC O objetivo deste notebook é carregar os dados do projeto Nintendo em uma extarnal table onde apenas será registrado novos registro a tabela. 
# MAGIC
# MAGIC 1 - faz leitura do arquivo json config para obter o nome da storageaccount em questão.
# MAGIC
# MAGIC 2 - obtém o caminho do notebook para identificar palavras referente ao ambiente e define a env em questão.
# MAGIC
# MAGIC 3 - importa funções do repositório meus_scripts_pyspark.
# MAGIC
# MAGIC 4 - Executa uma função sql que cria a external table com a location delta table e partition by file_date, caso a tabela não exista.
# MAGIC
# MAGIC 5 - chama a função que identifica novos registro, caso a tabela não exista, o dataframe silver é inserido a external table.
# MAGIC
# MAGIC 6 - carrega o dataframe de novos registro na external table
# MAGIC
# MAGIC 7 - Altera para inativo os valores de cada registro distinto da coluna id se seu file_date não for a data mais recente.

# COMMAND ----------

import os
import json
import sys

# COMMAND ----------

# Obtém o caminho do diretório atual do notebook
current_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())

# COMMAND ----------

current_dir = '/'.join(current_path.split('/')[:4])

# Define o caminho do arquivo de configuração
config_path = f"/Workspace{current_dir}/config.json"

# Abre o arquivo de configuração e carrega seu conteúdo em um dicionário
with open(config_path, "r") as f:
    config = json.load(f)

# Obtém o valor da chave "env" do dicionário de configuração
storage = config["storage"]

# COMMAND ----------

# Verifica se o caminho atual contém a string "dev"
if "dev" in current_path:
    # Define o ambiente como "dev"
    env = "dev"
# Verifica se o caminho atual contém a string "prd"
elif "prd" in current_path:
    # Define o ambiente como "prd"
    env = "prd"
# Caso contrário, define o ambiente como "env não encontrado"
else:
    env = "env não encontrado"

# COMMAND ----------

# Define o caminho do diretório Silver no Azure Data Lake Storage
silver_path = f'/Volumes/nintendo_databricks/{env}/silver-vol'

# Lê os dados do diretório Silver no formato Parquet e carrega em um DataFrame Spark
df = spark.read.parquet(silver_path)

# COMMAND ----------

# Cria uma consulta SQL para criar uma tabela externa no Delta Lake
query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {env}.`nintendo-bigtable` (
    id STRING,
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
# Ajusta o caminho do diretório para os primeiros 3 níveis
current_dir = '/'.join(current_path.split('/')[:3])

sys.path.append(f'/Workspace{current_dir}/meus_scripts_pyspark')

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
        (df["id"] == tabela["id"]) & (df["file_date"] != tabela["file_date"])
    ) | ~(df["id"] == tabela["id"])
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

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *

# Define o caminho para a tabela Delta
delta_path = f"abfss://{env}@{storage}.dfs.core.windows.net/gold"

# Carrega a tabela Delta usando o formato Delta
delta_df = spark.read.format("delta").load(delta_path)

# Encontra a data máxima para cada id
max_dates_df = delta_df.groupBy("id").agg(max("file_date").alias("max_file_date"))

# Cria a coluna 'status' com base na comparação de datas
delta_df = delta_df.join(max_dates_df, "id", "inner")
delta_df = delta_df.withColumn(
    "status",
    when(col("file_date") == col("max_file_date"), "ativado").otherwise("desativado")
).drop("max_file_date")

# Converte o DataFrame de volta para uma tabela Delta e salva as alterações
delta_table = DeltaTable.forPath(spark, delta_path)  # Usa forPath com o caminho
delta_table.alias("tabela_delta").merge(
    delta_df.alias("atualizacoes"),
    "tabela_delta.id = atualizacoes.id"
).whenMatchedUpdate(set={
    "status": "atualizacoes.status",

}).execute()
