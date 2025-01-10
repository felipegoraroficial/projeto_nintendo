# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo deste notebook
# MAGIC
# MAGIC  Este notebook tem como objetivo obter os registro de log do job do workflow do databricks. 
# MAGIC
# MAGIC 1- faz leitura do arquivo json config para obter o env em questão.
# MAGIC
# MAGIC 2- importa funções do repositório meus_scripts_pyspark.
# MAGIC
# MAGIC 3 - chama a função que faz a extração de dados do job.
# MAGIC
# MAGIC 4 - transforma os dados extraidos em um dataframe com o schema inserido.
# MAGIC
# MAGIC 5 - chama a função responsavel pela limpeza e transformação dos dados de jobs.
# MAGIC
# MAGIC 6 - salva os dados em uma tabela do catalog

# COMMAND ----------

import sys
import json
import os
from pyspark.sql.functions import from_unixtime, col, udf, regexp_extract, round, when, split, to_date, date_format
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

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

# Obtém o valor das chaves do dicionário de configuração
env = config["env"]
token = config["token"]
job_id = config["job"]
current_instance = config["instance"]

# COMMAND ----------

# Adiciona o caminho do diretório 'meus_scripts_pyspark' ao sys.path
# Isso permite que módulos Python localizados nesse diretório sejam importados
# Diretorio referente a funções de pyspark
sys.path.append(f'{current_dir}/meus_scripts_pyspark')

# COMMAND ----------

from get_job_runs_databricks import get_job_runs_databricks
from clean_job_data_databricks import clean_job_data_databricks

# COMMAND ----------

#define o schema dos dados que serão estraidos do job do databricks
schema = StructType([
    StructField("job_id", LongType(), True),
    StructField("run_id", LongType(), True),
    StructField("creator_user_name", StringType(), True),
    StructField("number_in_job", StringType(), True),
    StructField("original_attempt_run_id", StringType(), True),
    StructField("start_time", LongType(), True),
    StructField("setup_duration", LongType(), True),
    StructField("execution_duration", LongType(), True),
    StructField("cleanup_duration", LongType(), True),
    StructField("end_time", LongType(), True),
    StructField("run_duration", LongType(), True),
    StructField("trigger", StringType(), True),
    StructField("run_name", StringType(), True),
    StructField("run_page_url", StringType(), True),
    StructField("run_type", StringType(), True),
    StructField("format", StringType(), True),
    StructField("status", StructType([
        StructField("state", StringType(), True),
        StructField("termination_details", StructType([
            StructField("code", StringType(), True),
            StructField("type", StringType(), True),
            StructField("message", StringType(), True)
        ]),True)
    ]),True),
    StructField("job_run_id", LongType(), True)
])

# COMMAND ----------

#chamar função que extrai dados de jobs do databricks
log = get_job_runs_databricks(current_instance, token, job_id)

# COMMAND ----------

#transforma lista json em dataframe com o schema inserido
df = spark.createDataFrame(log, schema=schema)

# COMMAND ----------

#executa a função que realiza a limpeza e tratativa de dados do dataframe de jobs do databricks
df = clean_job_data_databricks(df)

# COMMAND ----------

#salva o dataframe em uma tabela do catalog do databricks
df.write.mode('overwrite').saveAsTable(f"{env}.`log-table`")
