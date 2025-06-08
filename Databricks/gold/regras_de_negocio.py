# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType 
from pyspark.sql.functions import concat_ws, col, regexp_replace, when, lit, desc, max, to_date, regexp_extract, count
from pyspark.sql import SparkSession
import re
import os

# COMMAND ----------

def read_all_delta_partitions(nome_tabela):
    
    spark = SparkSession.builder.getOrCreate()

    try:
        df = spark.read.format("delta").load(nome_tabela)
        
        # Obter o número total de linhas
        qtd = df.count()
        
        # Verificar se o DataFrame não está vazio
        assert qtd > 0, f"A tabela '{nome_tabela}' está vazia."
        
        print(f"Tabela '{nome_tabela}' carregada com sucesso. Número de linhas: {qtd}")
        return df
        
    except Exception as e:
        print(f"Erro ao acessar a tabela '{nome_tabela}': {e}")
        # Retorna um DataFrame vazio com o mesmo esquema em caso de erro
        # Tenta inferir o esquema lendo um pequeno pedaço se possível, ou retorna um esquema genérico
        try:
            # Tenta ler um pequeno pedaço para inferir o esquema se a tabela existe mas o carregamento falhou por outro motivo
            temp_df = spark.read.format("delta").load(nome_tabela)
            return spark.createDataFrame([], schema=temp_df.schema)
        except Exception:
            # Se não conseguir inferir o esquema, retorna um DataFrame vazio sem esquema definido ou com um esquema básico
            print("Não foi possível inferir o esquema da tabela. Retornando DataFrame vazio sem esquema.")
            return spark.createDataFrame([], "string") # Exemplo: um DataFrame vazio com uma coluna string

# COMMAND ----------

# Caminho para a external location do diretório silver
silver_path = f"/Volumes/nintendodatabricksplgwf5_workspace/nintendo/silver"

# Lendo arquivo Delta do diretório silver em todas partições
df = read_all_delta_partitions(silver_path)

# COMMAND ----------

display(df)
