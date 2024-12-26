# Databricks notebook source
import os
import json

# COMMAND ----------

# Obtém o caminho do diretório atual
current_dir = os.getcwd()

# COMMAND ----------

# Ajusta o caminho do diretório para os primeiros 4 níveis
current_dir = '/'.join(current_dir.split('/')[:4])

# COMMAND ----------

print(current_dir)

# COMMAND ----------

# Define o caminho do arquivo de configuração
config_path = f"{current_dir}/projeto_nintendo/config.json"

# COMMAND ----------

print(config_path)
