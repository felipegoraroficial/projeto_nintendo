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
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# Obtém o caminho do diretório atual do notebook
current_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())

# COMMAND ----------

current_dir = '/'.join(current_path.split('/')[:4])

# Define o caminho do arquivo de configuração
config_path = f"/Workspace{current_dir}/config/config.json"

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
    codigo STRING,
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

# Carregando tabela gold antiga
try:
    tabela = spark.read.table(f"nintendo_databricks.{env}.`nintendo-bigtable`")
except:
    pass

# Verifica se a tabela está vazia
if tabela.rdd.isEmpty():
    
    novos_registros = df

else:
    # Realize um join para encontrar os novos registros entre id e file dates diferentes
    def join_dataframes(df1, df2):

        df_joined1 = df1.join(df2, "id", "left_anti")

        # Transformando a coluna file_date_1 de tipo date para string
        df1 = df1.withColumn("file_date", col("file_date").cast("string"))

        # Transformando a coluna file_date_2 de tipo date para string
        df2 = df2.withColumn("file_date", col("file_date").cast("string"))

        df1 = df1.withColumnRenamed("file_date", "file_date_1").withColumn("id_file_date", concat(col("id").cast("string"), col("file_date_1").cast("string")))

        df2 = df2.withColumnRenamed("file_date", "file_date_2").withColumn("id_file_date", concat(col("id").cast("string"), col("file_date_2").cast("string")))

        # Transformando a coluna file_date_1 de tipo string para date
        df1 = df1.withColumn("file_date_1", col("file_date_1").cast("date"))

        # Transformando a coluna file_date_2 de tipo string para date
        df2 = df2.withColumn("file_date_2", col("file_date_2").cast("date"))

        # Fazendo o join pelo campo 'id' para encontrar registros correspondentes
        df_joined2 = df1.join(df2, on='id_file_date', how='inner')

        # Filtrando onde os 'file_date' são diferentes entre os dois DataFrames
        df_joined2 = df_joined2.filter(col("file_date_1") != col("file_date_2")).select(df1["*"]).drop("id_file_date")

        # Renomeando a coluna file_date_1 para file_date para manter a consistência
        df_joined2 = df_joined2.withColumnRenamed("file_date_1", "file_date")

        return df_joined1.union(df_joined2)

    novos_registros = join_dataframes(df, tabela)

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

# Define o caminho para a tabela Delta
delta_path = f"abfss://{env}@{storage}.dfs.core.windows.net/gold"

# Carrega a tabela Delta usando o formato Delta
delta_df = spark.read.format("delta").load(delta_path)

# Adiciona uma coluna de número de linha para identificar duplicados
window_spec = Window.partitionBy("codigo").orderBy(col("file_date").desc())
delta_df = delta_df.withColumn("row_number", row_number().over(window_spec))

# Identificando registros ativos
# Filtra apenas a primeira ocorrência de cada id
delta_df_ativo = delta_df.filter(col("row_number") == 1).drop("row_number")

# Define a coluna status como "ativo"
delta_df_ativo = delta_df_ativo.withColumn("status", lit("ativo"))

# Identificando registros inativos
# Filtra apenas a primeira ocorrência de cada id
delta_df_inativos = delta_df.filter(col("row_number") > 1).drop("row_number")

# Define a coluna status como "ativo"
delta_df_inativos = delta_df_inativos.withColumn("status", lit("inativo"))

# COMMAND ----------

# Salvando status como inativo
# Converte o DataFrame de volta para uma tabela Delta e salva as alterações
delta_table = DeltaTable.forPath(spark, delta_path)
delta_table.alias("tabela_delta").merge(
    delta_df_inativos.alias("atualizacoes"),
    "tabela_delta.id = atualizacoes.id"
).whenMatchedUpdate(set={
    "status": "atualizacoes.status",
}).execute()

# COMMAND ----------

# Salvando status como ativo
# Converte o DataFrame de volta para uma tabela Delta e salva as alterações
delta_table = DeltaTable.forPath(spark, delta_path)
delta_table.alias("tabela_delta").merge(
    delta_df_ativo.alias("atualizacoes"),
    "tabela_delta.id = atualizacoes.id"
).whenMatchedUpdate(set={
    "status": "atualizacoes.status",
}).execute()
