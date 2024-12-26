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

current_dir

# COMMAND ----------

# Define o caminho do arquivo de configuração
config_path = f"{current_dir}/projeto_nintendo/config.json"

# COMMAND ----------

config_path

# COMMAND ----------

# Abre o arquivo de configuração e carrega seu conteúdo em um dicionário
with open(config_path, "r") as f:
    config = json.load(f)

# Obtém o valor da chave "env" do dicionário de configuração
env = config["env"]

# COMMAND ----------

env
