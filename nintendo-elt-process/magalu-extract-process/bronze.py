# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo deste notebook
# MAGIC
# MAGIC  Este notebook tem como objetivo carregar e processar dados que estão em uma external location do storageaccount do azure em formato txt e com a utilização da biblioteca BeautifulSoup, que identifica os elementos dos dados extraídos em html que serão necesários para a realização do projeto, armazena-os em uma variavel para carregar os dados posteriormente em outra camada da external location em um formato json.

# COMMAND ----------

from bs4 import BeautifulSoup
import requests
import re
import json
import os
from datetime import datetime
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

# Caminho para o diretório de entrada
inbound_path = f"abfss://{env}@nintendostorageaccount.dfs.core.windows.net/magalu/inbound"

# Lista todos os arquivos no diretório de entrada que terminam com ".txt"
file_paths = [
    f"{inbound_path}/{file.name}" for file in dbutils.fs.ls(inbound_path) if file.name.endswith(".txt")
]

# COMMAND ----------

# Obtém a data atual no formato YYYY-MM-DD
data_atual = datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# Filtrando a lista com a data_atual 
currrent_files_path = [arquivo for arquivo in file_paths if data_atual in arquivo] 

# COMMAND ----------

if currrent_files_path:

    for file_path in file_paths:  # Itera sobre cada arquivo na lista de arquivos

        list_todos = []  # Inicializa uma lista vazia para armazenar os dados extraídos

        df = spark.read.text(file_path)
        html_content = "\n".join(row.value for row in df.collect())

        sopa_bonita = BeautifulSoup(html_content, 'html.parser')  # Analisa o conteúdo HTML usando BeautifulSoup

        list_links = sopa_bonita.find_all('a', {'data-testid': 'product-card-container'}) # Encontra todos os links de produtos # Extrair todos os hrefs dos links 
        links = [link['href'] for link in list_links if 'href' in link.attrs]

        for link in links:

            link = 'https://www.magazineluiza.com.br' + link

            # Define o cabeçalho do agente de usuário para a requisição HTTP
            headers = {'user-agent': 'Mozilla/5.0'}

            # Faz uma requisição GET para a URL especificada com o número da página e cabeçalho
            resposta = requests.get(link, headers=headers)

            # Analisa o conteúdo HTML da resposta usando BeautifulSoup
            sopa_bonita = BeautifulSoup(resposta.text, 'html.parser')

            titulo = sopa_bonita.find('h1', {'data-testid': 'heading-product-title'}).text

            preco = sopa_bonita.find('p', {'data-testid': 'price-value'}).text

            desconto = "sem desconto"

            if sopa_bonita.find('span', class_='sc-fyVfxW bBlpKX'):
                desconto = sopa_bonita.find('span', class_='sc-fyVfxW bBlpKX').text

            moeda = preco[0] + preco[1]

            parcelamento = sopa_bonita.find('p', class_='sc-dcJsrY bdQQwX sc-joQczN fWWRYL').text

            titulo, preco, desconto, moeda, parcelamento

            list_todos.append({
                'titulo': titulo,
                'moeda': moeda,
                'preco_promo': preco,
                'condition_promo': desconto,
                'parcelado': parcelamento,
                'link': link
            })  # Adiciona os dados extraídos à lista

        json_file_path = file_path.replace('inbound', 'bronze').replace('.txt', '.json')  # Define o caminho do arquivo JSON de saída
        dbutils.fs.put(json_file_path, json.dumps(list_todos, ensure_ascii=False, indent=4), overwrite=True)  # Salva os dados extraídos no arquivo JSON

else:
    print(f"Não existe arquivo extraído na data de {data_atual}")

# COMMAND ----------

#deletando arquivos que já possuem um tempo de armazenamento maior que 30 dias

path = f"abfss://{env}@nintendostorageaccount.dfs.core.windows.net/magalu/bronze"

deleting_files_range_30(path)
