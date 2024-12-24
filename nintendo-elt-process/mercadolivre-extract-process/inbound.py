# Databricks notebook source
import requests
from bs4 import BeautifulSoup
import datetime
import re
 

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

# Abre um arquivo para escrita no diretório especificado no DBFS, com nome baseado no número da página e data atual
with open(f"/dbfs/mnt/dev/mercadolivre/inbound/{current_date}.txt", "w") as file:
    # Escreve o conteúdo da página no arquivo
    file.write(page_content)
