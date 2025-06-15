# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType 
from pyspark.sql.functions import concat_ws, col, regexp_replace, when, lit, desc, max, to_date, regexp_extract, count
from pyspark.sql import SparkSession
import re
import os

# COMMAND ----------

def last_partition_delta(nome_tabela, coluna_particao):

   
    try:
        df = spark.read.format("delta").load(nome_tabela)
    except Exception as e:
        print(f"Erro ao acessar a tabela '{nome_tabela}': {e}")
        return spark.createDataFrame([], schema=df.schema if 'df' in locals() else [])

    ultima_particao_df = df.select(max(coluna_particao).alias("ultima_particao"))
    ultima_particao = ultima_particao_df.first()["ultima_particao"] if ultima_particao_df.first() else None

    if ultima_particao is not None:
        filtro = f"{coluna_particao} = '{ultima_particao}'"
        df_ultima_particao = df.where(filtro)
        print(f"Tabela '{nome_tabela}' filtrada pela última partição: {ultima_particao}")

        qtd = df_ultima_particao.count()

        assert qtd > 0, f"A última partição '{ultima_particao}' da tabela '{nome_tabela}' está vazia."
        
        print(f"Leitura da tabela '{nome_tabela}' carregada com sucesso. Número de linhas: {qtd}")

        return df_ultima_particao
    else:
        print(f"Não foram encontradas partições na tabela '{nome_tabela}'.")
        return spark.createDataFrame([], schema=df.schema)

# Caminho para a external location do diretório bronze
bronze_path = f"/Volumes/nintendodatabricksp1okle_workspace/nintendo/silver"

# Lendo arquivo Delta do diretório bronze pela ultima partição
df = last_partition_delta(bronze_path, "data_ref")

# COMMAND ----------

from pyspark.sql.functions import round

df_price_desconto = df.withColumn("preco_desconto", round(col("preco") - (col("preco") * col("desconto")), 2))

# COMMAND ----------

df_price_parcelado = df_price_desconto.withColumn("preco_parcelado", round((col("valor_prestacao") * col("numero_parcelas")), 2))


# COMMAND ----------

import requests
from datetime import datetime

def get_cdi_today():
    url = "https://brasilapi.com.br/api/taxas/v1/cdi"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        cdi = 1 + (data['valor'] / 100)
        return cdi
    except requests.exceptions.RequestException as e:
        print(f"Erro ao conectar à API da Brasil API: {e}")
        return None
    except (ValueError, IndexError) as e:
        print(f"Erro ao processar os dados da API: {e}")
        return None

cdi_data = get_cdi_today()
cdi_data

# COMMAND ----------

df_save_desconto = df_price_parcelado.withColumn("save_desconto", round((col("preco") - col("preco_desconto")), 2))

# COMMAND ----------

from pyspark.sql.functions import when, col, round

df_save_parcelado = df_save_desconto.withColumn(
    "save_parcelado",
    when((col("preco") - col("preco_parcelado")) * cdi_data <= 0, 0)
    .otherwise(round(((col("preco") - col("preco_parcelado")) * cdi_data), 2))
)

# COMMAND ----------

from pyspark.sql.functions import round

df_pct_parcelado = df_save_parcelado.withColumn(
    "pct_preco_parcelado",
    round(100 -((col("preco_parcelado") / col("preco")) * 100), 2)
)

# COMMAND ----------

from pyspark.sql.functions import when

df_recomendacao = df_pct_parcelado.withColumn(
    "recomendacao",
    when(col("desconto") < col("pct_preco_parcelado"), "Comprar Parcelado")
    .when(col("desconto") > col("pct_preco_parcelado"), "Comprar a Vista Pix")
    .otherwise("Sem Vantagem Identificada")
).drop("preco_desconto", "preco_parcelado", "save_desconto", "save_parcelado", "pct_preco_parcelado")

# COMMAND ----------

def carregando_tabela_gold(df,delta_table_path):

    print(f"Iniciando o salvamento do DataFrame no formato Delta em: {delta_table_path}")

    try:
        # --- Passo 1: Obter a contagem de linhas ANTES de salvar ---
        num_rows_to_save = df.count()
        print(f"Número de linhas no DataFrame a ser salvo: {num_rows_to_save}")

        # --- Passo 2: Salvar o DataFrame no formato Delta ---
        df.write \
                        .format("delta") \
                        .mode("overwrite") \
                        .partitionBy("data_ref") \
                        .save(delta_table_path)

        print(f"DataFrame salvo com sucesso como tabela Delta particionada por 'extract' em: {delta_table_path}")

        # --- Início das Verificações de Qualidade Pós-Gravação ---

        # --- 1. Garantir que os dados foram salvos no caminho ---
        print(f"\n--- Verificação: Leitura da Tabela Delta Salva ---")
        df_delta_read = spark.read.format("delta").load(delta_table_path)
        print("Esquema da tabela Delta lida:")
        df_delta_read.printSchema()
        print("Primeiras 5 linhas da tabela Delta lida:")
        df_delta_read.show(5, truncate=False)

        if df_delta_read.isEmpty():
            print(f"ALERTA: A tabela Delta salva em '{delta_table_path}' está vazia ou não pôde ser lida.")
        else:
            print(f"OK: A tabela Delta foi lida com sucesso de '{delta_table_path}'.")


        # --- 2. Verificar se a quantidade de linhas salvas condiz com o que está salvo ---
        num_rows_saved = df_delta_read.count()
        print(f"\n--- Verificação: Contagem de Linhas Salvas ---")
        print(f"Número de linhas salvas na tabela Delta: {num_rows_saved}")

        if num_rows_saved == num_rows_to_save:
            print(f"STATUS: OK - A quantidade de linhas salvas ({num_rows_saved}) corresponde à quantidade de linhas no DataFrame original ({num_rows_to_save}).")
        else:
            print(f"ALERTA: A quantidade de linhas salvas ({num_rows_saved}) NÃO CORRESPONDE à quantidade de linhas no DataFrame original ({num_rows_to_save}). Investigue!")


        # --- 3. Verificar se realmente foi particionado ---
        print(f"\n--- Verificação: Particionamento por 'data' ---")

        # Substituindo %fs ls -l por dbutils.fs.ls()
        print("Conteúdo do diretório Delta (buscando por pastas de partição usando dbutils):")
        try:
            # Lista os subdiretórios no caminho Delta. Esperamos ver pastas como data=YYYY-MM-DD
            delta_contents = dbutils.fs.ls(delta_table_path)
            partition_folders_found = [f.name for f in delta_contents if f.isDir and "=" in f.name]
            if partition_folders_found:
                print(f"Pastas de partição detectadas (ex: {', '.join(partition_folders_found[:3])}...):")
                # Opcional: mostrar todas as pastas de partição se for um número pequeno
                # for p_folder in partition_folders_found:
                #     print(f"  - {p_folder}")
            else:
                print("Nenhuma pasta de partição padrão (ex: 'data=...') detectada diretamente no caminho raiz.")

        except Exception as ls_e:
            print(f"ALERTA: Erro ao listar conteúdo do diretório Delta com dbutils.fs.ls(): {ls_e}")


        # O método mais confiável continua sendo usar o DESCRIBE DETAIL
        try:
            spark.sql(f"DESCRIBE DETAIL delta.`{delta_table_path}`").show(truncate=False)
            table_details_df = spark.sql(f"DESCRIBE DETAIL delta.`{delta_table_path}`")
            partition_columns = table_details_df.select("partitionColumns").collect()[0][0] # Pega o primeiro elemento da lista

            if "data_ref" in partition_columns:
                print(f"STATUS: OK - A tabela Delta está particionada pela coluna 'data_ref'.")
            else:
                print(f"ALERTA: A tabela Delta NÃO parece estar particionada pela coluna 'data_ref'. Partições encontradas: {partition_columns}")

        except Exception as sql_e:
            print(f"ALERTA: Não foi possível obter detalhes da tabela Delta (verifique o log): {sql_e}")


    except Exception as e:
        print(f"Ocorreu um erro geral ao salvar ou verificar a tabela Delta: {e}")

# Caminho para a external location do diretório gold
gold_path = f"/Volumes/nintendodatabricksp1okle_workspace/nintendo/gold"

carregando_tabela_gold(df_recomendacao, gold_path)
