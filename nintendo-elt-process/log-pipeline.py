# Databricks notebook source
import requests
import json
import os
from pyspark.sql.functions import from_unixtime, col, udf, regexp_extract, round, when, split, to_date, date_format
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

# COMMAND ----------

def get_job_runs(databricks_instance, token, job_id):

    url = f"https://{databricks_instance}/api/2.0/jobs/runs/list"

    headers = {
        "Authorization": f"Bearer {token}"
    }

    params = {"job_id": job_id}

    response = requests.get(url, headers=headers, params=params)

    response.raise_for_status()

    return response.json()

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

log = get_job_runs(current_instance, token, job_id)

# COMMAND ----------

logs_list = []

for run in log['runs']:
    logs_list.append(run)

# COMMAND ----------

logs_list

# COMMAND ----------

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

df = spark.createDataFrame(logs_list, schema=schema)

# COMMAND ----------

df = df.withColumn('start_time', (col('start_time') / 1000).cast('timestamp'))
df = df.withColumn('end_time', (col('end_time') / 1000).cast('timestamp'))

# COMMAND ----------

def convert_durantion(ms):

    seconds = ms // 1000
    minutes = seconds // 60
    remaining_seconds = seconds % 60

    return f'{minutes}m:{remaining_seconds}s'

convert_durantion_udf = udf(convert_durantion,StringType())

df = df.withColumn('run_duration', convert_durantion_udf(col('run_duration')))

# COMMAND ----------

df = df.withColumn('state', col('status.termination_details.code')) \
       .withColumn('message', col('status.termination_details.message')) \
       .drop('status')

# COMMAND ----------

df = df.withColumn('state', when(col('state') == 'RUN_EXECUTION_ERROR', 'FAILED').otherwise(col('state')))

# COMMAND ----------

df = df.withColumn('start_date', to_date(split(col('start_time'), ' ')[0], 'yyyy-MM-dd')) \
       .withColumn('start_time', date_format(split(col('start_time'), ' ')[1], 'HH:mm:ss.SSS'))

df = df.withColumn('end_date', to_date(split(col('end_time'), ' ')[0], 'yyyy-MM-dd')) \
       .withColumn('end_time', date_format(split(col('end_time'), ' ')[1], 'HH:mm:ss.SSS'))

# COMMAND ----------

df = df.select('run_name','run_id','creator_user_name','start_date','start_time','end_date','end_time','run_duration','trigger','run_page_url','state','message')

# COMMAND ----------

df = df.withColumn('run_duration_minutes', regexp_extract(col('run_duration'), '(\d+)m', 1).cast('double')) \
       .withColumn('run_duration_seconds', regexp_extract(col('run_duration'), '(\d+)s', 1).cast('double')) \
       .withColumn('run_duration', round(col('run_duration_minutes') + col('run_duration_seconds') / 60, 2)) \
       .drop('run_duration_minutes', 'run_duration_seconds')

# COMMAND ----------

df = df.orderBy(col('start_date').desc(), col('start_time').desc())

# COMMAND ----------

df.write.mode('overwrite').saveAsTable(f"{env}.`log-table`")
