import requests
from bs4 import BeautifulSoup
import os
import datetime
 
def get_data_magalu():

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    headers = {'user-agent': 'Mozilla/5.0'}

    html_pages = []

    base_path = "/home/fececa/airflow/dags/nintendo/data/extract"
    raw_data_path = os.path.join(base_path, "magalu/raw")

    for page_number in range(1, 17):

        resposta = requests.get(f"https://www.magazineluiza.com.br/busca/nintendo+switch/?page={page_number}", headers=headers)
        sopa = BeautifulSoup(resposta.text, 'html.parser')
        html_pages.append(sopa.prettify())


    os.makedirs(raw_data_path, exist_ok=True)

    with open(os.path.join(raw_data_path, f'{current_date}.txt'), 'w', encoding='utf-8') as file:
        file.write('\n'.join(html_pages))



