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
import requests

# COMMAND ----------

# Obtém a data atual no formato "YYYY-MM-DD"
current_date = datetime.datetime.now().strftime("%Y-%m-%d")

# define a url para scrapy
url = "https://www.amazon.com.br/b/?_encoding=UTF8&node=16253310011&bbn=7791985011&ref_=Oct_d_odnav_d_16253308011_2&pd_rd_w=EJcLa&content-id=amzn1.sym.6363238c-5dbd-413a-97c6-fd3fb2c70739&pf_rd_p=6363238c-5dbd-413a-97c6-fd3fb2c70739&pf_rd_r=J0J2YDWVG0CB33RCSR13&pd_rd_wg=32alb&pd_rd_r=e104dc19-5b42-458e-941a-abe5cea31cc2"

# Define o cabeçalho do agente de usuário para a requisição HTTP
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive'
}

# Faz uma requisição GET para a URL especificada com o número da página e cabeçalho
resposta = requests.get(url, headers=headers)

filetext = resposta.text

# COMMAND ----------

# define o caminho inbound da external location da storage account
file_path = f'/Volumes/nintendoworkspace/nintendoschema/inbound-vol/magalu/{current_date}.html'

# Escreve o conteúdo da página no arquivo usando dbutils.fs.put
dbutils.fs.put(file_path, filetext, overwrite=True)
