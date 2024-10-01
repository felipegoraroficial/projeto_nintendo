from bs4 import BeautifulSoup
import re
import json
import os

def bronze_data_magalu():

    directory_path = "/home/fececa/airflow/dags/nintendo/data/extract/magalu/raw/"

    for filename in os.listdir(directory_path):

        list_todos = []
        
        if filename.endswith(".txt"):
            file_path = os.path.join(directory_path, filename)
            
            with open(file_path, "r", encoding="utf-8") as file:
                html_content = file.read()


            sopa_bonita = BeautifulSoup(html_content, 'html.parser')

            list_titulo = sopa_bonita.find_all('h2', {'data-testid': 'product-title'})
            list_preco_promo = sopa_bonita.find_all('p', {'data-testid': 'price-value'})
            list_condition_promo = sopa_bonita.find_all('span', {'data-testid': 'in-cash'})
            list_parcelamento = sopa_bonita.find_all('p', {'data-testid': 'installment'})
            img_tags = sopa_bonita.find_all('img')
            img_srcs = [img['src'] for img in img_tags]

            for titulo, preco_promo, condition_promo, parcelado, img in zip(list_titulo, list_preco_promo, list_condition_promo, list_parcelamento, img_srcs):

                titulo = titulo.text.strip()
                moeda = preco_promo.text.strip()
                moeda = moeda[0] + moeda[1]
                preco_promo = preco_promo.text.strip().replace('R$', '').replace('\xa0', '').replace('.', '').replace(',', '.')
                condition_promo = condition_promo.text.strip()
                
                parcelado_text = re.search(r'\d+\.\d+', parcelado.text)
                parcelado = parcelado_text.group() if parcelado_text else ''

                imagem = str(img).replace('[', '').replace(']', '')

                list_todos.append({
                    'titulo': titulo,
                    'moeda': moeda,
                    'preco_promo': preco_promo,
                    'condition_promo': condition_promo,
                    'parcelado': parcelado,
                    'imagem': imagem
                })



        output_folder = "/home/fececa/airflow/dags/nintendo/data/bronze/magalu"
        file_name_sem_extensao = filename.replace(".txt", "")
        output_file_name = f"{file_name_sem_extensao}.json"
        output_file_path = os.path.join(output_folder, output_file_name)

        os.makedirs(output_folder, exist_ok=True)

        with open(output_file_path, 'w', encoding="utf-8") as json_file:
            json.dump(list_todos, json_file, ensure_ascii=False, indent=4)