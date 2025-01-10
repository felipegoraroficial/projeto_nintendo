# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo do Notebook
# MAGIC  Este notebook tem como objetivo processar e analisar dados relacionados ao projeto Nintendo.
# MAGIC  Ele carrega um arquivo de configuração para definir o ambiente de execução e utiliza bibliotecas do PySpark para manipulação e transformação dos dados.
# MAGIC
# MAGIC  O notebook está dividido em várias células, cada uma com uma função específica:
# MAGIC  1. Importação das bibliotecas necessárias.
# MAGIC  2. Carregamento do arquivo de configuração e definição do ambiente de execução.
# MAGIC  3. Leitura e processamento dos dados.
# MAGIC  4. Transformações e limpeza dos dados.
# MAGIC  5. Carregamento dos dados em outra camada da external lcoation no storageaccount da azure.

# COMMAND ----------

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

# Adiciona o caminho do diretório 'meus_scripts_pyspark' ao sys.path
# Isso permite que módulos Python localizados nesse diretório sejam importados
# Diretorio referente a funções de pyspark
sys.path.append(f'{current_dir}/meus_scripts_pyspark')

# COMMAND ----------

from organize_files import process_data_to_bronze
from change_null_string import change_null_string
from change_null_numeric import change_null_numeric
from union_df import union_df
from remove_extra_spaces import remove_extra_spaces
from lower_string_column import lower_string_column
from convert_currency_column import convert_currency_column
from type_monetary import type_monetary
from replace_characters import replace_characters
from extract_characters import extract_characters
from filter_like import filter_like
from extract_memory import extract_memory
from condition_like import condition_like

# COMMAND ----------

# Adiciona o caminho do diretório 'meus_scripts_pytest' ao sys.path
# Isso permite que módulos Python localizados nesse diretório sejam importados
# Diretorio referente a funções de pytest
sys.path.append(f'{current_dir}/meus_scripts_pytest')

# COMMAND ----------

from df_not_empty import df_not_empty
from schema_equals_df_schema import schema_equals_df_schema

# COMMAND ----------

# Definindo o esquema para o DataFrame
schema = StructType([
    StructField("titulo", StringType(), True),          # Título do produto
    StructField("moeda", StringType(), True),           # Moeda utilizada na transação
    StructField("condition_promo", StringType(), True), # Condição promocional do produto
    StructField("preco_promo", DoubleType(), True),      # Preço promocional do produto
    StructField("parcelado", StringType(), True),        # Valor parcelado do produto
    StructField("link", StringType(), True),          # URL do link do produto
    StructField("origem", StringType(), True),          # Origem da extração do produto
    StructField("file_date", DateType(), True),          # Data do arquivo
    StructField("status", StringType(), True),          # Status do registro
])

# COMMAND ----------

# Caminho para a external location do diretório bronze em magalu
bronze_path = f'/Volumes/nintendo_databricks/{env}/magalu-vol/bronze/'

# Lendo arquivos JSON do diretório bronze com a opção de multiline ativada
mg = spark.read.option("multiline", "true").json(bronze_path)

mg = process_data_to_bronze(mg,'link')

# COMMAND ----------

df_not_empty(mg)

# COMMAND ----------

# Caminho para a external location do diretório bronze em mercadolivre
bronze_path = f'/Volumes/nintendo_databricks/{env}/mercadolivre-vol/bronze/'

# Lendo arquivos JSON do diretório bronze com a opção de multiline ativada
ml = spark.read.option("multiline", "true").json(bronze_path)

ml = process_data_to_bronze(ml,'link')

# COMMAND ----------

df_not_empty(ml)

# COMMAND ----------

df = union_df(ml, mg)

# COMMAND ----------

# Seleciona as colunas específicas do DataFrame para manter no resultado final
df = df.select('titulo', 'moeda', 'condition_promo', 'preco_promo', 'parcelado', 'link', 'file_name', 'file_date', 'status')

# COMMAND ----------

df = type_monetary(df, "preco_promo")

# COMMAND ----------

df = replace_characters(df, "condition_promo", r"[()]", "")
df = replace_characters(df, "condition_promo", "de desconto no pix", "OFF")

# COMMAND ----------

df = convert_currency_column(df, 'preco_promo')

# COMMAND ----------

# Cria um novo DataFrame com base no RDD do DataFrame existente e aplica o esquema especificado
df = spark.createDataFrame(df.rdd, schema)

# COMMAND ----------

schema_equals_df_schema(df,schema)

# COMMAND ----------

# Função para remover espaços em branco extras de todas as colunas de string
df = remove_extra_spaces(df)


# COMMAND ----------

# Converter valores nulos de colunas que são do tipo double para 0
df = change_null_numeric(df, 'double')

# COMMAND ----------

# Converte valores nulos de colunas que são do tipo string para -
df = change_null_string(df)

# COMMAND ----------

df = extract_characters(df,'origem','origem',rf'dbfs:/Volumes/nintendo_databricks/{env}/(.*?)-vol/bronze/')

# COMMAND ----------

df = filter_like(df,"titulo","(?i)^console.*switch")

# COMMAND ----------

df = extract_memory(df, 'titulo')


# COMMAND ----------

df = condition_like(df, 'oled', 'titulo', '(?i)Oled')
df = condition_like(df, 'lite', 'titulo', '(?i)Lite')

# COMMAND ----------

df = lower_string_column(df, 'memoria')

# COMMAND ----------

df_not_empty(df)

# COMMAND ----------

# Caminho para a external location do diretório silver
silver_path = f'/Volumes/nintendo_databricks/{env}/silver-vol'

# Salva o DataFrame em formato parquet na external location
df.write.mode("overwrite").parquet(silver_path)
