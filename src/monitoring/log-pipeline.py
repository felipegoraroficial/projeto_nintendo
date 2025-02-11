# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo deste notebook
# MAGIC
# MAGIC  Este notebook tem como objetivo obter os registro de log do job do workflow do databricks. 
# MAGIC
# MAGIC 1- faz leitura do arquivo json config para obter valores de atributos ao workflow em questão.
# MAGIC
# MAGIC 2 - obtém o caminho do notebook para identificar palavras referente ao ambiente e define a env em questão.
# MAGIC
# MAGIC 3- importa funções do repositório meus_scripts_pyspark.
# MAGIC
# MAGIC 4 - chama a função que faz a extração de dados do job.
# MAGIC
# MAGIC 5 - transforma os dados extraidos em um dataframe com o schema inserido.
# MAGIC
# MAGIC 6 - chama a função responsavel pela limpeza e transformação dos dados de jobs.
# MAGIC
# MAGIC 7 - salva os dados em uma tabela do catalog

# COMMAND ----------

import sys
import json
import os
from pyspark.sql.functions import from_unixtime, col, udf, regexp_extract, round, when, split, to_date, date_format
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

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

# Obtém o valor das chaves do dicionário de configuração
token = config["token"]
job_id = config["job"]
current_instance = config["instance"]

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

# Adiciona o caminho do diretório 'meus_scripts_pyspark' ao sys.path
# Isso permite que módulos Python localizados nesse diretório sejam importados
# Ajusta o caminho do diretório para os primeiros 3 níveis
current_dir = '/'.join(current_path.split('/')[:3])

sys.path.append(f'/Workspace{current_dir}/meus_scripts_pyspark')

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
