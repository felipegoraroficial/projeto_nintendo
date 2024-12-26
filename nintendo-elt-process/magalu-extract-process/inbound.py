# Databricks notebook source
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

    # Abre um arquivo para escrita no diretório especificado no DBFS, com nome baseado no número da página e data atual
    with open(f"/dbfs/mnt/{env}/magalu/inbound/page{page_number}_{current_date}.txt", "w") as file:
        # Escreve o conteúdo da página no arquivo
        file.write(page_content)
