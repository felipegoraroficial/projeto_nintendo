# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo deste notebook
# MAGIC
# MAGIC  Este notebook tem como objetivo carregar e processar dados que estão em uma external location do storageaccount do azure em formato txt e com a utilização da biblioteca BeautifulSoup, que identifica os elementos dos dados extraídos em html que serão necesários para a realização do projeto. 
# MAGIC
# MAGIC 1- obtém o caminho do notebook para identificar palavras referente ao ambiente e define a env em questão.
# MAGIC
# MAGIC 2 - cria uma lista com os nomes de arquivos que estão armazenados no volume.
# MAGIC
# MAGIC 3 - define a data atual e identifica se existe um arquivo que contém a data atual no nome para processar apenas o arquivo mais atualizado.
# MAGIC
# MAGIC 4 - obtem o href no arquivo em questão para extrair informacoes do anuncio usando BeautifulSoup para elementos como titulo, preço, moeda, parcelamento e armazena os dados em uma lista que será carregada no formato parquet no volume camada bronze

# COMMAND ----------

from bs4 import BeautifulSoup
import requests
import sys
import os
from datetime import datetime

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

from create_unique_file import create_unique_file

# COMMAND ----------

# Caminho para o diretório de entrada
inbound_path = f"/Volumes/nintendo_databricks/{env}/magalu-vol/inbound"

# Lista todos os arquivos no diretório de entrada que terminam com ".txt"
file_paths = [
    f"{inbound_path}/{file.name}" for file in dbutils.fs.ls(inbound_path) if file.name.endswith(".txt")
]

# COMMAND ----------

# Obtém a data atual no formato YYYY-MM-DD
data_atual = datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# Filtrando a lista com a data_atual 
currrent_files_path = next((arquivo for arquivo in file_paths if data_atual in arquivo), None)

# COMMAND ----------

if currrent_files_path:  # Verifica se há um caminho de arquivo atual

    list_todos = []  # Inicializa uma lista vazia para armazenar os dados extraídos

    df = spark.read.text(currrent_files_path)  # Lê o arquivo de texto no caminho atual como um DataFrame Spark
    html_content = "\n".join(row.value for row in df.collect())  # Concatena o conteúdo das linhas do DataFrame em uma única string

    sopa_bonita = BeautifulSoup(html_content, 'html.parser')  # Analisa o conteúdo HTML usando BeautifulSoup

    list_links = sopa_bonita.find_all('a', {'data-testid': 'product-card-container'})  # Encontra todos os links de produtos
    links = [link['href'] for link in list_links if 'href' in link.attrs]  # Extrai todos os hrefs dos links

    for link in links:  # Itera sobre cada link

        link = 'https://www.magazineluiza.com.br' + link  # Concatena a URL base com o link do produto

        headers = {'user-agent': 'Mozilla/5.0'}  # Define o cabeçalho do agente de usuário para a requisição HTTP

        resposta = requests.get(link, headers=headers)  # Faz uma requisição GET para a URL especificada com o cabeçalho

        sopa_bonita = BeautifulSoup(resposta.text, 'html.parser')  # Analisa o conteúdo HTML da resposta usando BeautifulSoup

        if sopa_bonita.find('span', class_='sc-dcJsrY daMqkh'): # Verifica se há um elemento de codigo do produto
            codigo = sopa_bonita.find('span', class_='sc-dcJsrY daMqkh').text  # Extrai o codigo do produto
        elif sopa_bonita.find('span', class_='sc-iGgWBj eXbWIe'): # Verifica se há um elemento de codigo do produto
            codigo = sopa_bonita.find('span', class_='sc-iGgWBj eXbWIe').text  # Extrai o codigo do produto
        else:
            codigo = "sem código"

        titulo = sopa_bonita.find('h1', {'data-testid': 'heading-product-title'}).text  # Extrai o título do produto

        preco = sopa_bonita.find('p', {'data-testid': 'price-value'}) # Extrai o preço do produto
        preco = preco.text if preco else "R$ 0,00" # Se não encontrado texto no elemento, retorne R$0,00


        if sopa_bonita.find('span', class_='sc-fyVfxW bBlpKX'):  # Verifica se há um elemento de desconto
            desconto = sopa_bonita.find('span', class_='sc-fyVfxW bBlpKX').text  # Extrai o valor do desconto
        elif sopa_bonita.find('span', class_='sc-eHsDsR bYZWfg'):  # Verifica se há um elemento de desconto
            desconto = sopa_bonita.find('span', class_='sc-eHsDsR bYZWfg').text  # Extrai o valor do desconto
        else:
            desconto = "sem desconto"  # Define o valor padrão para desconto

        moeda = preco[0] + preco[1]  # Extrai a moeda do preço

        if sopa_bonita.find('p', class_='sc-dcJsrY bdQQwX sc-joQczN fWWRYL'):  # Verifica se há um elemento de parcelamento
            parcelamento = sopa_bonita.find('p', class_='sc-dcJsrY bdQQwX sc-joQczN fWWRYL').text  # Extrai o valor do parcelamento
        elif sopa_bonita.find('p', class_='sc-dcJsrY bdQQwX sc-kobALw yIiQA'):  # Verifica se há um elemento alternativo de parcelamento
            parcelamento = sopa_bonita.find('p', class_='sc-dcJsrY bdQQwX sc-kobALw yIiQA').text  # Extrai o valor do parcelamento alternativo
        elif sopa_bonita.find('p', class_='sc-iGgWBj idhlOX sc-SrznA hXFzWz'):  # Verifica se há um elemento alternativo de parcelamento
            parcelamento = sopa_bonita.find('p', class_='sc-iGgWBj idhlOX sc-SrznA hXFzWz').text  # Extrai o valor do parcelamento alternativo
        else:
            parcelamento = "sem parcelamento"  # Define o valor padrão para parcelamento

        list_todos.append({  # Adiciona os dados extraídos à lista
            'codigo': codigo,
            'titulo': titulo,
            'moeda': moeda,
            'preco_promo': preco,
            'condition_promo': desconto,
            'parcelado': parcelamento,
            'link': link
        })

else:
    # Imprime uma mensagem caso não exista arquivo extraído na data atual
    print(f"Não existe arquivo extraído na data de {data_atual}")

# COMMAND ----------

# Cria um RDD a partir da lista de dicionários
rdd = spark.sparkContext.parallelize(list_todos)

# Converte o RDD em um DataFrame
df = rdd.toDF()

# COMMAND ----------

# Caminho para a external location do diretório bronze
bronze_path = f"/Volumes/nintendo_databricks/{env}/magalu-vol/bronze/{data_atual}"

# Salva o DataFrame Spark no arquivo Parquet
df.coalesce(1).write.mode('overwrite').parquet(bronze_path)

# COMMAND ----------

create_unique_file(bronze_path, 'parquet')
