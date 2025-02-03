# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo do Notebook
# MAGIC
# MAGIC Este notebook tem como objetivo realizar a extração de dados relacionados ao projeto Nintendo. Utilizando bibliotecas como `requests` e `BeautifulSoup`.
# MAGIC
# MAGIC 1- obtém o caminho do notebook para identificar palavras referente ao ambiente e define a env em questão.
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
import json
import os
import sys

# COMMAND ----------

# Obtém o caminho do diretório atual do notebook
current_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())

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

from req_bsoup import req_bsoup

# COMMAND ----------

# Obtém a data atual no formato "YYYY-MM-DD"
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# define a url para scrapy
url= "https://www.magazineluiza.com.br/nintendo/games/s/ga/ntdo/"

# chama a função para extração de dados brutos da url
page_content = req_bsoup(url)

# define o caminho inbound da external location da storage account
file_path = f'/Volumes/nintendo_databricks/{env}/magalu-vol/inbound/{current_date}.txt'

# Escreve o conteúdo da página no arquivo usando dbutils.fs.put
dbutils.fs.put(file_path, page_content, overwrite=True)
