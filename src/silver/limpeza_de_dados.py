# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType 
from pyspark.sql.functions import concat_ws, col, regexp_replace, when, lit, desc, max, to_date, regexp_extract, count
from pyspark.sql import SparkSession
import re
import os

# COMMAND ----------

def last_partition_delta(nome_tabela, coluna_particao):

    spark = SparkSession.builder.getOrCreate()
    
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
bronze_path = f"/Volumes/nintendodatabricksx9y9jj_workspace/nintendo/bronze"

# Lendo arquivo Delta do diretório bronze pela ultima partição
df = last_partition_delta(bronze_path, "data_ref")

# COMMAND ----------

def filter_not_null_value(df, coluna):

    print(f"Iniciando o filtro de valores vazios na coluna: {coluna}")

    dffiltered = df.filter(col(coluna).isNotNull())

    qtddotal = df.count()
    qtdnotnull = df.filter(col(coluna).isNotNull()).count()
    qtdnull = df.filter(col(coluna).isNull()).count()

    print(f"dataframe filtrado, numero de linhas: {qtdnotnull}")

    assert qtddotal == (qtdnull + qtdnotnull), \
    f"Erro na contagem: O total de linhas ({qtddotal}) não é igual à soma de nulos ({qtdnull}) e não nulos ({qtdnotnull}) para a coluna '{coluna}'."

    print(f"Filtro ralizado com sucesso")
    print(f"df origem {qtddotal} linhas = df filtrado {qtdnotnull} linhas + df não filtrado {qtdnull} linhas")

    return dffiltered

dffiltered = filter_not_null_value(df, "codigo")

# COMMAND ----------

display(dffiltered)

# COMMAND ----------

def define_data_columns(df):

    formato_regex = r"^\d{4}-\d{2}-\d{2}$"

    colunas_string = [coluna for coluna, dtype in df.dtypes if dtype == "string"]  

    print(f"Colunas strings identificadas no dataframes: {colunas_string}")

    for coluna in colunas_string:
        df_sem_nulos = df.filter(col(coluna).isNotNull())

        match_count = df_sem_nulos.filter(regexp_extract(col(coluna), formato_regex, 0) != "").count()
        total_count = df_sem_nulos.count()

        if match_count == total_count:  

            print(f"Coluna com padrões de data para a conversão: {coluna}")

            df = df.withColumn(coluna, to_date(col(coluna), "yyyy-MM-dd"))

            novo_tipo = dict(df.dtypes)[coluna]
            assert novo_tipo == "date", f"Erro: A coluna {coluna} não foi convertida corretamente! Tipo atual: {novo_tipo}"

            print(f"Coluna {coluna} convertida com sucesso, tipo identificado = {novo_tipo}")

    return df

df_convert_data = define_data_columns(dffiltered)

# COMMAND ----------

def tratar_parcelamento(df):

    # Extraia o número de parcelas (já tratando nulos)
    df_com_parcelas = df.withColumn(
        "numero_parcelas",
        when(col("parcelamento").isNotNull(), regexp_extract(col("parcelamento"), r'(\d+)x', 1)).otherwise(lit(0))
    )
    df_com_parcelas = df_com_parcelas.withColumn("numero_parcelas", col("numero_parcelas").cast(LongType()))

    # Extraia o valor da prestação (já tratando nulos e convertendo para Double)
    df_com_valores = df_com_parcelas.withColumn(
        "valor_prestacao",
        when(col("parcelamento").isNotNull(), regexp_extract(col("parcelamento"), r'R\$ (\d+,\d{2})', 1)).otherwise(lit("0"))
    ).withColumn(
        "valor_prestacao",
        when(col("valor_prestacao") != '0',
            regexp_replace(col("valor_prestacao"), ",", ".").cast(DoubleType())
        ).otherwise(lit(0.0))
    )

    # Remova a coluna original "parcelamento"
    df_final = df_com_valores.drop("parcelamento")

    return df_final

df_parcelado = tratar_parcelamento(df_convert_data)

# COMMAND ----------

def define_numeric_columns(df):
    # Regex para identificar valores percentuais e monetários
    regex_percentual = re.compile(r"^\d+%$")
    regex_monetario = re.compile(r"^R\$?\s?\d{1,3}(\.\d{3})*(,\d{2})?$")

    # Obtendo colunas de tipo string
    colunas_string = [coluna for coluna, dtype in df.dtypes if dtype == "string"]
    colunas_percentuais = []
    colunas_monetarias = []

    # Identifica colunas com valores percentuais e monetários
    for coluna in colunas_string:
        df_sem_nulos = df.filter(col(coluna).isNotNull())
        valores_amostra = df_sem_nulos.select(coluna).rdd.map(lambda row: row[0]).collect()

        if any(bool(regex_percentual.match(str(valor))) for valor in valores_amostra):
            print(f"A coluna '{coluna}' contém valores no formato percentual.")
            colunas_percentuais.append(coluna)

        if any(bool(regex_monetario.match(str(valor))) for valor in valores_amostra):
            print(f"A coluna '{coluna}' contém valores no formato monetário.")
            colunas_monetarias.append(coluna)

    # Aplica a conversão para valores percentuais
    for coluna in colunas_percentuais:
        df = df.withColumn(
            coluna,
            when(
                col(coluna).rlike("^\d+%$"),
                (regexp_replace(col(coluna), "%", "").cast(DoubleType()) / 100)
            ).otherwise(col(coluna))
        ).withColumn(coluna, col(coluna).cast(DoubleType()))

        print(f"Coluna {coluna} convertida com sucesso para tipo 'double'.")

    # Aplica a conversão para valores monetários
    for coluna in colunas_monetarias:
        df = df.withColumn(
            coluna,
            when(
                col(coluna).rlike("^R\\$?\\s?\\d{1,3}(\\.\\d{3})*(,\\d{2})?$"),
                regexp_replace(
                    regexp_replace(
                        regexp_replace(col(coluna), "R\\$", ""), 
                        "\\.", "" 
                    ),
                    ",", "."  
                ).cast(DoubleType())
            ).otherwise(col(coluna))
        ).withColumn(coluna, col(coluna).cast(DoubleType()))

        print(f"Coluna {coluna} convertida com sucesso para tipo 'double'.")

    return df

df_numeric = define_numeric_columns(df_parcelado)

# COMMAND ----------

def replace_nulls_with_zero(df):
    # Identificar colunas numéricas (inteiras e decimais)
    numeric_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, (LongType, DoubleType))]

    print(f"Colunas numéricas identificadas: {numeric_cols}")

    # Contar valores nulos antes da transformação
    null_counts_before = df.select([count(when(col(c).isNull(), c)).alias(c) for c in numeric_cols]).collect()[0].asDict()
    print(f"Valores nulos antes da transformação: {null_counts_before}")

    # Substituir valores nulos por 0 nas colunas numéricas
    for col_name in numeric_cols:
        df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
        print(f"Valor nulo na coluna {col_name} alterado para 0")

    # Contar valores nulos depois da transformação
    null_counts_after = df.select([count(when(col(c).isNull(), c)).alias(c) for c in numeric_cols]).collect()[0].asDict()
    print(f"Valores nulos depois da transformação: {null_counts_after}")

    # Verificar se todas as colunas tiveram seus valores nulos substituídos
    for col_name in numeric_cols:
        if null_counts_after[col_name] == 0:
            print(f"✅ Coluna {col_name} foi corretamente preenchida.")
        else:
            print(f"⚠️ Coluna {col_name} ainda contém valores nulos!")

    return df

df_no_null_numeric = replace_nulls_with_zero(df_numeric)

# COMMAND ----------

display(df_no_null_numeric)

# COMMAND ----------

def replace_nulls_with_hyphen(df):
    # Identificar colunas numéricas (inteiras e decimais)
    string_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, (StringType))]

    print(f"Colunas numéricas identificadas: {string_cols}")

    # Contar valores nulos antes da transformação
    null_counts_before = df.select([count(when(col(c).isNull(), c)).alias(c) for c in string_cols]).collect()[0].asDict()
    print(f"Valores nulos antes da transformação: {null_counts_before}")

    # Substituir valores nulos por 0 nas colunas numéricas
    for col_name in string_cols:
        df = df.withColumn(col_name, when(col(col_name).isNull(), '-').otherwise(col(col_name)))
        print(f"Valor nulo na coluna {col_name} alterado para '-'")

    # Contar valores nulos depois da transformação
    null_counts_after = df.select([count(when(col(c).isNull(), c)).alias(c) for c in string_cols]).collect()[0].asDict()
    print(f"Valores nulos depois da transformação: {null_counts_after}")

    # Verificar se todas as colunas tiveram seus valores nulos substituídos
    for col_name in string_cols:
        if null_counts_after[col_name] == 0:
            print(f"✅ Coluna {col_name} foi corretamente preenchida.")
        else:
            print(f"⚠️ Coluna {col_name} ainda contém valores nulos!")

    return df

df_no_null_string = replace_nulls_with_hyphen(df_no_null_numeric)

# COMMAND ----------

def extract_memory(df, column_name):
    
    def extract_memory_info(info):

        if isinstance(info, str) and info:
            padrao = r'(\d+)\s*(G[Bb])'
            resultado = re.search(padrao, info, re.IGNORECASE)
            if resultado:
                return resultado.group(0)
        return '-'

    extrair_memoria_udf = udf(extract_memory_info, StringType())
    
    return df.withColumn('memoria', extrair_memoria_udf(col(column_name)))

df_memory_list = extract_memory(df_no_null_string, 'nome')

# COMMAND ----------

def condition_like(df, new_column_name, condition_column, pattern):
    
    
    df = df.withColumn(new_column_name, when(col(condition_column).rlike(pattern), 'Sim').otherwise('Nao'))

    return df

df_old = condition_like(df_memory_list, 'oled', 'nome', '(?i)Oled')
df_lite = condition_like(df_old, 'lite', 'nome', '(?i)Lite')

# COMMAND ----------

def carregando_tabela_silver(df,delta_table_path):

    print(f"Iniciando o salvamento do DataFrame no formato Delta em: {delta_table_path}")

    try:
        # --- Passo 1: Obter a contagem de linhas ANTES de salvar ---
        num_rows_to_save = df.count()
        print(f"Número de linhas no DataFrame a ser salvo: {num_rows_to_save}")

        # --- Passo 2: Salvar o DataFrame no formato Delta ---
        df.write \
                        .format("delta") \
                        .mode("append") \
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

# Caminho para a external location do diretório silver
silver_path = f"/Volumes/nintendodatabricksx9y9jj_workspace/nintendo/silver"

carregando_tabela_silver(df_lite, silver_path)

