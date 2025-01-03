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
import json
import os

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

# Obtém a data atual no formato "YYYY-MM-DD"
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# Define o cabeçalho do agente de usuário para a requisição HTTP
headers = {'user-agent': 'Mozilla/5.0'}

# Loop para iterar sobre os números de página de 1 a 16
for page_number in range(1, 17):

    # Faz uma requisição GET para a URL especificada com o número da página e cabeçalho
    resposta = requests.get(f"https://www.magazineluiza.com.br/busca/nintendo+switch/?page={page_number}", headers=headers)
    
    # Analisa o conteúdo HTML da resposta usando BeautifulSoup
    sopa = BeautifulSoup(resposta.text, 'html.parser')
    
    # Formata o conteúdo da página HTML de forma legível
    page_content = sopa.prettify()

    # define o caminho inbound da external location da storage account
    file_path = f"abfss://{env}@nintendostorageaccount.dfs.core.windows.net/magalu/inbound/page{page_number}_{current_date}.txt"

    # Escreve o conteúdo da página no arquivo usando dbutils.fs.put
    dbutils.fs.put(file_path, page_content, overwrite=True)
