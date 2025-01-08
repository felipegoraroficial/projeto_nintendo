# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo do Notebook
# MAGIC
# MAGIC Este notebook tem como objetivo realizar a extração de dados relacionados ao projeto Nintendo. Utilizando bibliotecas como `requests` e `BeautifulSoup`, o notebook faz a coleta de dados da web, processa e transforma esses dados conforme necessário, e finalmente carrega os dados processados em um formato adequado para análise posterior. Além disso, o notebook utiliza um arquivo de configuração (`config.json`) para definir parâmetros importantes, como o ambiente de execução.

# COMMAND ----------

import requests
from bs4 import BeautifulSoup
import datetime
import re
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

# COMMAND ----------

# Obtém a data atual no formato "YYYY-MM-DD"
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Define o cabeçalho do agente de usuário para a requisição HTTP
headers = {'user-agent': 'Mozilla/5.0'}

# Faz uma requisição GET para a URL especificada com o número da página e cabeçalho
resposta = requests.get(f"https://lista.mercadolivre.com.br/nintendo-sitwitch", headers=headers)

# Analisa o conteúdo HTML da resposta usando BeautifulSoup
sopa = BeautifulSoup(resposta.text, 'html.parser')

# Formata o conteúdo da página HTML de forma legível
page_content = sopa.prettify()

# define o caminho inbound da external location da storage account
file_path = f"/Volumes/nintendo_databricks/{env}/mercadolivre-vol/inbound/{current_date}.txt"


# Escreve o conteúdo da página no arquivo usando dbutils.fs.put
dbutils.fs.put(file_path, page_content, overwrite=True)

# COMMAND ----------

#deletando arquivos que já possuem um tempo de armazenamento maior que 30 dias

path = f"/Volumes/nintendo_databricks/{env}/mercadolivre-vol/inbound"

deleting_files_range_30(path)
