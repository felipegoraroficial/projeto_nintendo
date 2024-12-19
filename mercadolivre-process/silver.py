# Databricks notebook source
from pyspark.sql.functions import input_file_name, when, col, regexp_extract, to_date, row_number,udf, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

# COMMAND ----------

# Definindo o esquema para o DataFrame
schema = StructType([
    StructField("titulo", StringType(), True),          # Título do produto
    StructField("moeda", StringType(), True),           # Moeda utilizada na transação
    StructField("condition_promo", StringType(), True), # Condição promocional do produto
    StructField("preco_promo", FloatType(), True),      # Preço promocional do produto
    StructField("parcelado", FloatType(), True),        # Valor parcelado do produto
    StructField("imagem", StringType(), True),          # URL da imagem do produto
    StructField("file_date", DateType(), True)          # Data do arquivo
])

# COMMAND ----------

# Caminho para o diretório bronze
bronze_path = "/mnt/dev/mercadolivre/bronze/"

# Lendo arquivos JSON do diretório bronze com a opção de multiline ativada
df = spark.read.option("multiline", "true").json(bronze_path)

# COMMAND ----------

display(df)

# COMMAND ----------

# Adiciona uma coluna com o nome do arquivo e extrai a data do arquivo a partir do nome do arquivo
df = df.withColumn("file_name", input_file_name()) \
       .withColumn("file_date", to_date(regexp_extract(col("file_name"), r'\d{4}-\d{2}-\d{2}', 0), "yyyy-MM-dd"))

# Define uma janela de partição para ordenar por file_date em ordem decrescente e filtra para manter apenas a linha mais recente para cada imagem
window_spec = Window.partitionBy("imagem").orderBy(col("file_date").desc())
df = df.withColumn("row_number", row_number().over(window_spec)) \
       .filter(col("row_number") == 1) \
       .drop("row_number")

# Seleciona as colunas desejadas e converte as colunas parcelado e preco_promo para float
df = df.select("titulo", "moeda", "condition_promo", "preco_promo", "parcelado", "imagem", "file_date") \
       .withColumn("parcelado", col("parcelado").cast("float")) \
       .withColumn("preco_promo", col("preco_promo").cast("float"))

# Substitui valores nulos ou 'nan' por '-' nas colunas de string
string_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]
for col_name in string_cols:
    df = df.withColumn(col_name, when(col(col_name).isNull() | (col(col_name) == 'nan'), '-').otherwise(col(col_name)))

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

# Caminho para o diretório silver
silver_path = "/mnt/dev/mercadolivre/silver/"

# Salva o DataFrame em formato parquet no DBFS em silver_path
df.write.mode("overwrite").parquet(silver_path)
