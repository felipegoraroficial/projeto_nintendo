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

# MAGIC %pip install openai langchain-openai langchain

# COMMAND ----------

import os
import requests
from bs4 import BeautifulSoup
import json
import re
import os
from datetime import datetime
from pyspark.dbutils import DBUtils
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import StrOutputParser
from pyspark.sql.types import StructType, StructField, StringType

# Inicializando dbutils
dbutils = DBUtils(spark)

ai_key = os.environ.get("OPENAI_API")

# COMMAND ----------

# Obtém a data atual no formato YYYY-MM-DD
data_atual = datetime.now().strftime("%Y-%m-%d")

# Caminho para o diretório de entrada
inbound_path = f"/Volumes/nintendoworkspace/nintendoschema/inbound-vol/amazon"

# Lista todos os arquivos no diretório de entrada que terminam com ".html"
file_paths = [
    f"{inbound_path}{file.name}" for file in dbutils.fs.ls(inbound_path) if file.name.endswith(".html")
]

# Filtrando a lista com a data_atual 
currrent_files_path = next((arquivo for arquivo in file_paths if data_atual in arquivo), None)

# COMMAND ----------

if currrent_files_path:  # Verifica se há um caminho de arquivo atual

    list_todos = []  # Inicializa uma lista vazia para armazenar os dados extraídos

    # Lendo o conteúdo do arquivo
    local_file_path = "/tmp/pagina_busca.html" # Caminho local temporário no driver node
    dbutils.fs.cp(currrent_files_path, f"file:{local_file_path}")
    with open(local_file_path, "r") as f:
        html_content = f.read()

    sopa_bonita = BeautifulSoup(html_content, 'html.parser')

# COMMAND ----------

# Criar um agente LangChain para interagir com o DataFrame
llm = ChatOpenAI(temperature=0.7, model="gpt-4o-mini", openai_api_key=ai_key)

# COMMAND ----------

prompt = ChatPromptTemplate.from_messages([
    ("system", "Você é um especialista em extrair informações relevantes de conteúdo HTML. Analise o conteúdo fornecido e capture as informações solicitadas pelo usuário."),
    ("human", "Por favor, analise o seguinte conteúdo HTML:\n\n{html_conteudo}\n\nE capture os dados no seguinte formato:\n\n```json\n{{\n  \"produtos\": [\n    {{\n      \"nome\": \"[nome do produto 1]\",\n      \"preco\": \"[preço do produto 1]\",\n      \"link\": \"[link do produto 1]\",\n      \"codigo\": \"[código do produto 1]\",\n      \"desconto\": \"[desconto do produto 1]\",\n      \"parcelamento\": \"[parcelamento do produto 1]\"\n    }},\n    {{\n      \"nome\": \"[nome do produto 2]\",\n      \"preco\": \"[preço do produto 2]\",\n      \"link\": \"[link do produto 2]\",\n      \"codigo\": \"[código do produto 2]\",\n      \"desconto\": \"[desconto do produto 2]\",\n      \"parcelamento\": \"[parcelamento do produto 2]\"\n    }},\n    ...\n  ]\n}}\n```\n\nCapture [todos os nomes de produtos, seus preços, links relacionados, códigos dos produtos, descontos e informações de parcelamento] e formate a saída seguindo rigorosamente a estrutura JSON fornecida. Certifique-se de que a lista de produtos esteja corretamente formatada. Caso não encontre o objeto solicitado, retorne como nulo para cada objeto."),
])

# COMMAND ----------

chain = prompt | llm | StrOutputParser()

# Passando o conteúdo HTML (como string) do objeto BeautifulSoup para o prompt
resposta = chain.invoke({"html_conteudo": str(sopa_bonita)})

# COMMAND ----------

match = re.search(r"\{(.*)\}", resposta, re.DOTALL)
if match:
    json_string = "{" + match.group(0) + "}" 

    # Remove as chaves duplas, substituindo por chaves simples
    json_string = json_string.replace('{{', '{').replace('}}', '}')

    dados = json.loads(json_string)

    if 'produtos' in dados and isinstance(dados['produtos'], list):
        for produto in dados['produtos']:
            codigo = produto.get('codigo')
            nome = produto.get('nome')
            preco = produto.get('preco')
            desconto = produto.get('desconto')
            parcelamento = produto.get('parcelamento')
            link = produto.get('link')
            list_todos.append({
                'codigo': codigo,
                'nome': nome,
                'preco': preco,
                'desconto': desconto,
                'parcelamento': parcelamento,
                'link': link,
                'data':data_atual
            })


else:
    print("Nenhuma estrutura JSON delimitada por chaves encontrada.")

# COMMAND ----------

# Cria um RDD a partir da lista de dicionários
rdd = spark.sparkContext.parallelize(list_todos)

# Define o schema do DataFrame
schema = StructType([
    StructField("codigo", StringType(), True),
    StructField("nome", StringType(), True),
    StructField("preco", StringType(), True),  
    StructField("desconto", StringType(), True),  
    StructField("parcelamento", StringType(), True), 
    StructField("link", StringType(), True),
    StructField("data", StringType(), True)
])

# Converte o RDD em um DataFrame
df = rdd.toDF(schema)

# COMMAND ----------

# Caminho para a external location do diretório bronze
bronze_path = f"/Volumes/nintendoworkspace/nintendoschema/bronze-vol/amazon"

# Salva o DataFrame Spark no formato delta
df.write.format("delta") \
    .partitionBy("data") \
    .mode("append") \
    .save(bronze_path)
