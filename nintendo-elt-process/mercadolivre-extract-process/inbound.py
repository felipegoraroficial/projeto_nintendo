# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo do Notebook
# MAGIC
# MAGIC Este notebook tem como objetivo realizar a extração de dados relacionados ao projeto Nintendo. Utilizando bibliotecas como `requests` e `BeautifulSoup`.
# MAGIC
# MAGIC 1- faz leitura do arquivo json config para obter o env em questão.
# MAGIC
# MAGIC 2- importa funções do repositório meus_scripts_pyspark.
# MAGIC
# MAGIC 3 - define a data atual para passar o nome do arquivo extraido.
# MAGIC
# MAGIC 4 - chama a função req_bsoup para extrair dados html referente a url informada
# MAGIC
# MAGIC 5 - define o file path para carregar dados usando dbutils.
# MAGIC
# MAGIC 6 - chama a função deleting_files_range_30_days para excluir arquivos com mais de 30 dias armazenados

# COMMAND ----------

import datetime
import os
import json
import sys

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

# Obtém o valor da chave "env" do dicionário de configuração
env = config["env"]

# COMMAND ----------

# Adiciona o caminho do diretório 'meus_scripts_pyspark' ao sys.path
# Isso permite que módulos Python localizados nesse diretório sejam importados
sys.path.append(f'{current_dir}/meus_scripts_pyspark')

# COMMAND ----------

from deleting_files_range_30_days import deleting_files_range_30
from req_bsoup import req_bsoup

# COMMAND ----------

# Obtém a data atual no formato "YYYY-MM-DD"
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# define a url para scrapy
url = "https://lista.mercadolivre.com.br/nintendo-sitwitch"

# chama a função para extração de dados brutos da url
page_content = req_bsoup(url)

# define o caminho inbound da external location da storage account
file_path = f"/Volumes/nintendo_databricks/{env}/mercadolivre-vol/inbound/{current_date}.txt"

# Escreve o conteúdo da página no arquivo usando dbutils.fs.put
dbutils.fs.put(file_path, page_content, overwrite=True)

# COMMAND ----------

#deletando arquivos que já possuem um tempo de armazenamento maior que 30 dias

path = f"/Volumes/nintendo_databricks/{env}/mercadolivre-vol/inbound"

deleting_files_range_30(path)
