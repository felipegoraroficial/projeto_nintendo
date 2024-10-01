import requests
from bs4 import BeautifulSoup
import re
import os
import datetime

def get_data_ml():
 
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    headers = {'user-agent': 'Mozilla/5.0'}

    base_path = "/home/fececa/airflow/dags/nintendo/data/extract"
    raw_data_path = os.path.join(base_path, "ml/raw")

    html_pages = []

    url = 'https://lista.mercadolivre.com.br/nintendo-sitwitch'
    resposta = requests.get(url, headers=headers)
    sopa_bonita = BeautifulSoup(resposta.text, 'html.parser')

    link_tags = sopa_bonita.find_all('a')
    filtered_links = [link for link in link_tags if 'nintendo switch' in link.get('title', '').lower() and not re.search(r'lista', link.get('href', '').lower())]

    for link_tag in filtered_links:
        link = link_tag.get('href')

        resposta_produto = requests.get(link)
        sopa_produto = BeautifulSoup(resposta_produto.text, 'html.parser')
        html_pages.append(sopa_produto.prettify())

    os.makedirs(raw_data_path, exist_ok=True)

    with open(os.path.join(raw_data_path, f'{current_date}.txt'), 'w', encoding='utf-8') as file:
        file.write('\n'.join(html_pages))




