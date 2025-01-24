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
# MAGIC 4 - obtem o href no arquivo em questão para extrair informacoes do anuncio usando BeautifulSoup para elementos como titulo, preço, moeda, parcelamento e armazena os dados em uma lista que será carregada no formato json no volume camada bronze

# COMMAND ----------

from bs4 import BeautifulSoup
import requests
import json
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

# Caminho para o diretório de entrada
inbound_path = f"/Volumes/nintendo_databricks/{env}/mercadolivre-vol/inbound"

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

if currrent_files_path:
    # Inicializa uma lista vazia para armazenar os dados extraídos
    list_todos = []

    # Lê o arquivo de texto no caminho especificado
    df = spark.read.text(currrent_files_path)
    # Concatena o conteúdo do arquivo em uma única string
    html_content = "\n".join(row.value for row in df.collect())

    # Analisa o conteúdo HTML usando BeautifulSoup
    sopa_bonita = BeautifulSoup(html_content, 'html.parser')

    # Encontra todos os elementos <h2> com a classe especificada
    list_titulo = sopa_bonita.find_all('h3', {'class': 'poly-component__title-wrapper'})

    # Itera sobre cada elemento encontrado
    for i in list_titulo:
        # Extrai o link do elemento <a> dentro do <h2>
        link = i.find('a')['href']

        # Define o cabeçalho do agente de usuário para a requisição HTTP
        headers = {'user-agent': 'Mozilla/5.0'}

        # Faz uma requisição GET para a URL especificada com o cabeçalho
        resposta = requests.get(link, headers=headers)

        # Analisa o conteúdo HTML da resposta usando BeautifulSoup
        sopa_bonita = BeautifulSoup(resposta.text, 'html.parser')

        # Verifica se o produto está indisponível
        if sopa_bonita.find('div', class_='ui-pdp-shipping-message__text'):
            print('Este produto está indisponível')
        else:
            # Extrai o título do produto
            titulo = sopa_bonita.find('h1', class_='ui-pdp-title').text

            # Encontra o elemento que contém o preço
            preco = sopa_bonita.find('div', class_='ui-pdp-price__second-line')

            # Extrai o valor do preço
            preco_valor = preco.find('span', {'data-testid': 'price-part'}).text

            # Verifica se há desconto e extrai o valor do desconto
            desconto = preco.find('span', class_='andes-money-amount__discount')
            desconto = desconto.text if desconto else "sem desconto"

            # Extrai o símbolo da moeda
            moeda = preco.find('span', class_='andes-money-amount__currency-symbol').text

            # Verifica se há informações de parcelamento e extrai o texto correspondente
            if sopa_bonita.find('p', class_='ui-pdp-color--GREEN ui-pdp-size--MEDIUM ui-pdp-family--REGULAR'):
                parcelamento = sopa_bonita.find('p', class_='ui-pdp-color--GREEN ui-pdp-size--MEDIUM ui-pdp-family--REGULAR').text
            elif sopa_bonita.find('p', class_='ui-pdp-color--BLACK ui-pdp-size--MEDIUM ui-pdp-family--REGULAR'):
                parcelamento = sopa_bonita.find('p', class_='ui-pdp-color--BLACK ui-pdp-size--MEDIUM ui-pdp-family--REGULAR').text
            else:
                parcelamento = "sem parcelamento"

            # Adiciona os dados extraídos à lista
            list_todos.append({
                'titulo': titulo,
                'moeda': moeda,
                'preco_promo': preco_valor,
                'condition_promo': desconto,
                'parcelado': parcelamento,
                'link': link
            })

else:
    # Imprime uma mensagem caso não exista arquivo extraído na data atual
    print(f"Não existe arquivo extraído na data de {data_atual}")

# COMMAND ----------

json_file_path = currrent_files_path.replace('inbound', 'bronze').replace('.txt', '.json')  # Define o caminho do arquivo JSON de saída
dbutils.fs.put(json_file_path, json.dumps(list_todos, ensure_ascii=False, indent=4), overwrite=True)  # Salva os dados extraídos no arquivo JSON
