# Databricks notebook source
from pyspark.sql.functions import input_file_name, when, col, regexp_extract, to_date, row_number,udf, lit,regexp_replace,trim
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
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

# COMMAND ----------

sys.path.append(f'{current_dir}/meus_scripts_pyspark')

# COMMAND ----------

from organize_files import process_data_to_bronze
from change_null_string import change_null_string
from change_null_numeric import change_null_numeric

# COMMAND ----------

# Definindo o esquema para o DataFrame
schema = StructType([
    StructField("titulo", StringType(), True),          # Título do produto
    StructField("moeda", StringType(), True),           # Moeda utilizada na transação
    StructField("condition_promo", StringType(), True), # Condição promocional do produto
    StructField("preco_promo", DoubleType(), True),      # Preço promocional do produto
    StructField("parcelado", StringType(), True),        # Valor parcelado do produto
    StructField("imagem", StringType(), True),          # URL da imagem do produto
    StructField("file_name", StringType(), True),          # Origem da extração do produto
    StructField("file_date", DateType(), True)          # Data do arquivo
])

# COMMAND ----------

# Caminho para a external location do diretório bronze em magalu
bronze_path = f'abfss://{env}@nintendostorageaccount.dfs.core.windows.net/magalu/bronze/'

# Lendo arquivos JSON do diretório bronze com a opção de multiline ativada
mg = spark.read.option("multiline", "true").json(bronze_path)

mg = process_data_to_bronze(mg,'imagem')

# COMMAND ----------

# Caminho para a external location do diretório bronze em mercadolivre
bronze_path = f'abfss://{env}@nintendostorageaccount.dfs.core.windows.net/mercadolivre/bronze/'

# Lendo arquivos JSON do diretório bronze com a opção de multiline ativada
ml = spark.read.option("multiline", "true").json(bronze_path)

ml = process_data_to_bronze(ml,'imagem')

# COMMAND ----------

def unir_dataframes(df1, df2):

    return df1.unionByName(df2, allowMissingColumns=True)

df = unir_dataframes(ml, mg)

# COMMAND ----------

df = df.select('titulo', 'moeda', 'condition_promo','preco_promo', 'parcelado', 'imagem', 'file_name', 'file_date')

# COMMAND ----------

df = df.withColumn('preco_promo', regexp_replace(trim(col('preco_promo')), r'[^\d,]', '').cast('double'))

# COMMAND ----------

df = spark.createDataFrame(df.rdd, schema)

# COMMAND ----------

df = df.withColumn('parcelado', regexp_replace(trim(col('parcelado')), r'\s+', ' '))

# COMMAND ----------

df = change_null_numeric(df, 'double')

# COMMAND ----------

df = change_null_string(df)

# COMMAND ----------

# Função para extrair a memória do título do produto e registra a função como UDF
def extrair_memoria(info):
    import re
    if isinstance(info, str) and info:
        padrao = r'(\d+)\s*(G[gBb])'
        resultado = re.search(padrao, info)
        if resultado:
            return resultado.group(0)
    return '-'

extrair_memoria_udf = udf(extrair_memoria, StringType())

# Adiciona a coluna 'memoria' extraída do título do produto e colunas 'oled', 'lite' e 'joy_con' baseadas em padrões regex no título do produto
df = df.withColumn('memoria', extrair_memoria_udf(col('titulo'))) \
       .withColumn('oled', when(col('titulo').rlike('(?i)Oled'), 'Sim').otherwise('Nao')) \
       .withColumn('lite', when(col('titulo').rlike('(?i)Lite'), 'Sim').otherwise('Nao')) \
       .withColumn('joy_con', when(col('titulo').rlike('(?i)Joy-con'), 'Sim').otherwise('Nao'))

# COMMAND ----------

df = df.filter(col("titulo").rlike("(?i)^console"))

# COMMAND ----------

# Caminho para a external location do diretório silver
silver_path = f'abfss://{env}@nintendostorageaccount.dfs.core.windows.net/silver/'

# Salva o DataFrame em formato parquet na external location
df.write.mode("overwrite").parquet(silver_path)
