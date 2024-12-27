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
inbound_path = f"abfss://{env}@nintendostorageaccount.dfs.core.windows.net/magalu/inbound"

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

    list_titulo = sopa_bonita.find_all('h2', {'data-testid': 'product-title'})  # Encontra todos os títulos de produtos
    list_preco_promo = sopa_bonita.find_all('p', {'data-testid': 'price-value'})  # Encontra todos os preços promocionais
    list_condition_promo = sopa_bonita.find_all('span', {'data-testid': 'in-cash'})  # Encontra todas as condições promocionais
    list_parcelamento = sopa_bonita.find_all('p', {'data-testid': 'installment'})  # Encontra todas as informações de parcelamento
    img_tags = sopa_bonita.find_all('img')  # Encontra todas as tags de imagem
    img_srcs = [img['src'] for img in img_tags]  # Extrai os URLs das imagens

    for titulo, preco_promo, condition_promo, parcelado, img in zip(list_titulo, list_preco_promo, list_condition_promo, list_parcelamento, img_srcs):
        # Itera sobre os dados extraídos, combinando títulos, preços, condições, parcelamentos e URLs de imagens

        titulo = titulo.text.strip()  # Remove espaços em branco do título
        moeda = preco_promo.text.replace(" ", "").replace("\n", "").replace("ou", "")  # Remove todos os espaços vazios, quebras de linha e "ou" da variável preço_promo
        moeda = moeda[0] + moeda[1]  # Extrai a moeda do preço
        preco_promo = preco_promo.text.strip().replace('R$', '').replace('\xa0', '').replace('.', '').replace(',', '.')  # Formata o preço promocional
        condition_promo = condition_promo.text.strip()  # Remove espaços em branco da condição promocional
        
        parcelado_text = re.search(r'\d+\.\d+', parcelado.text)  # Encontra o valor do parcelamento usando regex
        parcelado = parcelado_text.group() if parcelado_text else ''  # Extrai o valor do parcelamento ou define como vazio

        imagem = str(img).replace('[', '').replace(']', '')  # Formata o URL da imagem

        list_todos.append({
            'titulo': titulo,
            'moeda': moeda,
            'preco_promo': preco_promo,
            'condition_promo': condition_promo,
            'parcelado': parcelado,
            'imagem': imagem
        })  # Adiciona os dados extraídos à lista

    json_file_path = file_path.replace('inbound', 'bronze').replace('.txt', '.json')  # Define o caminho do arquivo JSON de saída
    dbutils.fs.put(json_file_path, json.dumps(list_todos, ensure_ascii=False, indent=4), overwrite=True)  # Salva os dados extraídos no arquivo JSON
