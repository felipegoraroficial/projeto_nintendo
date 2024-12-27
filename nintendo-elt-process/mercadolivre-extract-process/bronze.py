# Databricks notebook source
from bs4 import BeautifulSoup
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

# Caminho para o diretório de entrada
inbound_path = f"abfss://{env}@nintendostorageaccount.dfs.core.windows.net/mercadolivre/inbound"

# Lista todos os arquivos no diretório de entrada que terminam com ".txt"
file_paths = [
    f"{inbound_path}/{file.name}" for file in dbutils.fs.ls(inbound_path) if file.name.endswith(".txt")
]

# COMMAND ----------

for file_path in file_paths:  # Itera sobre cada arquivo na lista de arquivos

    list_todos = []  # Inicializa uma lista vazia para armazenar os dados extraídos

    df = spark.read.text(file_path)
    html_content = "\n".join(row.value for row in df.collect())

    sopa_bonita = BeautifulSoup(html_content, 'html.parser')  # Analisa o conteúdo HTML usando BeautifulSoup

    list_titulo = sopa_bonita.find_all('h2', {'class': 'poly-box poly-component__title'})
    list_preco_promo = sopa_bonita.find_all('div', {'class': 'poly-price__current'})  # Encontra todos os preços promocionais e condições promocionais
    list_parcelamento = sopa_bonita.find_all('span', {'class': 'poly-price__installments poly-text-positive'})  # Encontra todas as informações de parcelamento
    img_tags = sopa_bonita.find_all('img')
    img_srcs = [img['src'] for img in img_tags]  # Extrai os URLs das imagens

    for titulo, preco_promo, parcelado, img in zip(list_titulo, list_preco_promo, list_parcelamento, img_srcs):
        # Itera sobre os dados extraídos, combinando títulos, preços, condições, parcelamentos e URLs de imagens

        titulo = titulo.text.strip()  # Remove espaços em branco do título
        moeda = preco_promo.find('span', class_='andes-money-amount__currency-symbol').text.strip()
        condition_promo = preco_promo.find('span', class_='andes-money-amount__discount poly-price__disc_label')
        condition_promo = condition_promo.text.strip() if condition_promo else "Sem Desconto"
        preco_promo = preco_promo.find('span', class_='andes-money-amount__fraction')
        preco_promo = preco_promo.text.replace(" ", "").replace("\n", "")
        parcelado = parcelado.text.strip()
        parcelado = parcelado.replace("\n", "")
        if not img.startswith('https://'):
            img = '-'
        else:
            img = img

        list_todos.append({
            'titulo': titulo,
            'moeda': moeda,
            'preco_promo': preco_promo,
            'condition_promo': condition_promo,
            'parcelado': parcelado,
            'imagem': img
        })  # Adiciona os dados extraídos à lista

    json_file_path = file_path.replace('inbound', 'bronze').replace('.txt', '.json')  # Define o caminho do arquivo JSON de saída
    dbutils.fs.put(json_file_path, json.dumps(list_todos, ensure_ascii=False, indent=4), overwrite=True)  # Salva os dados extraídos no arquivo JSON

