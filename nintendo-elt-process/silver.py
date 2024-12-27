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

# Adiciona o caminho do diretório 'meus_scripts_pyspark' ao sys.path
# Isso permite que módulos Python localizados nesse diretório sejam importados
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
    """
    Une dois DataFrames pelo nome das colunas, permitindo colunas ausentes.

    Parâmetros:
    df1 (DataFrame): O primeiro DataFrame.
    df2 (DataFrame): O segundo DataFrame.

    Retorna:
    DataFrame: Um novo DataFrame resultante da união dos dois DataFrames de entrada.
    """
    return df1.unionByName(df2, allowMissingColumns=True)

df = unir_dataframes(ml, mg)

# COMMAND ----------

# Seleciona as colunas específicas do DataFrame para manter no resultado final
df = df.select('titulo', 'moeda', 'condition_promo', 'preco_promo', 'parcelado', 'imagem', 'file_name', 'file_date')

# COMMAND ----------

# Remove caracteres não numéricos e vírgulas da coluna 'preco_promo', 
# em seguida, converte o valor para o tipo double
df = df.withColumn('preco_promo', regexp_replace(trim(col('preco_promo')), r'[^\d,]', '').cast('double'))

# COMMAND ----------

# Cria um novo DataFrame com base no RDD do DataFrame existente e aplica o esquema especificado
df = spark.createDataFrame(df.rdd, schema)

# COMMAND ----------

# Remove espaços em branco extras da coluna 'parcelado'
df = df.withColumn('parcelado', regexp_replace(trim(col('parcelado')), r'\s+', ' '))

# COMMAND ----------

# Converter valores nulos de colunas que são do tipo double para 0
df = change_null_numeric(df, 'double')

# COMMAND ----------

# Converte valores nulos de colunas que são do tipo string para -
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

# Filtra o DataFrame para manter apenas as linhas onde a coluna 'titulo' começa com a palavra 'console' (case insensitive)
df = df.filter(col("titulo").rlike("(?i)^console"))

# COMMAND ----------

# Caminho para a external location do diretório silver
silver_path = f'abfss://{env}@nintendostorageaccount.dfs.core.windows.net/silver/'

# Salva o DataFrame em formato parquet na external location
df.write.mode("overwrite").parquet(silver_path)
