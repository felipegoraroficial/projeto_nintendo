import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests
import os
from bs4 import BeautifulSoup
import json
from datetime import datetime
import re
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import StrOutputParser

def main(myTimer: func.TimerRequest, outputEvent: func.Out[str]) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    url = "https://www.kabum.com.br/gamer/nintendo/consoles-nintendo"
    ai_key = os.environ.get("OPENAI_API")
    storage_connection_string = os.environ.get("AzureStorageConnection")
    container_name = "nintendo"

    def get_html(url):

        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
        }
        
        try:
            resposta = requests.get(url, headers=headers)
            resposta.raise_for_status()
            return resposta.text
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao obter HTML da URL: {e}")
            return None

    def get_content(filetext):

        if not filetext:
            return []

        sopa_bonita = BeautifulSoup(filetext, 'html.parser')

        llm = ChatOpenAI(temperature=0.7, model="gpt-4o-mini", openai_api_key=ai_key)

        prompt = ChatPromptTemplate.from_messages([
        ("system", "Você é um especialista em extrair informações relevantes de conteúdo HTML. Analise o conteúdo fornecido e capture as informações solicitadas pelo usuário."),
        ("human", "Por favor, analise o seguinte conteúdo HTML:\n\n{html_conteudo}\n\nE capture os dados no seguinte formato:\n\n```json\n{{\n  \"produtos\": [\n    {{\n      \"nome\": \"[nome do produto 1]\",\n      \"preco\": \"[preço do produto 1]\",\n      \"link\": \"[link do produto 1]\",\n      \"codigo\": \"[código do produto 1]\",\n      \"desconto\": \"[desconto do produto 1]\",\n      \"parcelamento\": \"[parcelamento do produto 1]\"\n    }},\n    {{\n      \"nome\": \"[nome do produto 2]\",\n      \"preco\": \"[preço do produto 2]\",\n      \"link\": \"[link do produto 2]\",\n      \"codigo\": \"[código do produto 2]\",\n      \"desconto\": \"[desconto do produto 2]\",\n      \"parcelamento\": \"[parcelamento do produto 2]\"\n    }},\n    ...\n  ]\n}}\n```\n\nCapture [todos os nomes de produtos, seus preços, links relacionados, códigos dos produtos, descontos e informações de parcelamento] e formate a saída seguindo rigorosamente a estrutura JSON fornecida. Certifique-se de que a lista de produtos esteja corretamente formatada. Caso não encontre o objeto solicitado, retorne como nulo para cada objeto."),
        ])

        chain = prompt | llm | StrOutputParser()

        resposta = chain.invoke({"html_conteudo": str(sopa_bonita)})

        match = re.search(r"\{(.*)\}", resposta, re.DOTALL)
        if match:
            json_string = "{" + match.group(0) + "}" 

            json_string = json_string.replace('{{', '{').replace('}}', '}')

            try:
                dados = json.loads(json_string)
            except json.JSONDecodeError as e:
                logging.error(f"Erro ao decodificar JSON: {e}, string JSON: {json_string}")
                return []

            list_todos = []

            data_atual = datetime.now().strftime("%Y-%m-%d %H:%M")

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
                        'origem': 'kabum',
                        'extract':data_atual
                    })

            return list_todos
        else:
            logging.warning("Nenhum JSON encontrado na resposta da LLM.")
            return []

    filetext = get_html(url)
    if filetext:
        list_todos = get_content(filetext)
        if list_todos:
            # Envia um JSON por produto para o Event Hub
            for produto in list_todos:
                outputEvent.set(json.dumps(produto, ensure_ascii=False))
        else:
            logging.warning("Nenhum conteúdo para enviar ao Event Hub.")