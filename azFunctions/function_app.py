import logging
import azure.functions as func
from azure.eventhub import EventHubProducerClient, EventData
import requests
import os
from bs4 import BeautifulSoup
import json
from datetime import datetime
import re
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import StrOutputParser

app = func.FunctionApp()

@app.timer_trigger(schedule="0 */10 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def request_web(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    url = "https://www.kabum.com.br/gamer/nintendo/consoles-nintendo"
    ai_key = os.environ.get("OPENAI_API")
    eventhub_connection_string = os.environ.get("EVENTHUB_CONNECTION_STRING")
    eventhub_name = os.environ.get("EVENTHUB_NAME")

    def get_html(url):

        headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
        }
        
        resposta = requests.get(url, headers=headers)

        filetext = resposta.text

        return filetext

    def get_content(filetext):

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

            dados = json.loads(json_string)

            list_todos = []

            data_atual = datetime.now().strftime("%Y-%m-%d")

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

        return list_todos
            
    def send_producer(list_todos):

        if not eventhub_connection_string or not eventhub_name:
            logging.error("Por favor, certifique-se de que as variáveis de ambiente 'EVENTHUB_CONNECTION_STRING' e 'EVENTHUB_NAME' estão definidas.")
            return

        logging.info(f"Conectando ao Event Hub '{eventhub_name}'...")
        producer = EventHubProducerClient.from_connection_string(
            conn_str=eventhub_connection_string,
            eventhub_name=eventhub_name
        )

        with producer:
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(list_todos))
            producer.send_batch(event_data_batch)

        logging.info(f"Mensagem enviada com sucesso para o Event Hub '{eventhub_name}'!")

    filetext = get_html(url)


    list_todos = get_content(filetext)


    #send_producer(list_todos)