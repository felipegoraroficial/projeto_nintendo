# Databricks notebook source
import requests
import json
from pyspark.sql.functions import from_unixtime, col, udf
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

config_path = "/Workspace/nintendo/dev/projeto_nintendo/config.json"

with open(config_path, "r") as f:
    config = json.load(f)

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
