from bs4 import BeautifulSoup
import re
import json
import os

def bronze_data_ml():

    directory_path = "/home/fececa/airflow/dags/nintendo/data/extract/ml/raw/"

    for filename in os.listdir(directory_path):

        list_todos = []
        
        if filename.endswith(".txt"):
            file_path = os.path.join(directory_path, filename)
            
            with open(file_path, "r", encoding="utf-8") as file:
                html_content = file.read()

                sopa_bonita = BeautifulSoup(html_content, 'html.parser')

                list_titulo = sopa_bonita.find_all('h1', {'class': 'ui-pdp-title'})
                list_moeda = sopa_bonita.find_all('span', {'class': 'andes-money-amount__currency-symbol'})
                list_preco_promo = sopa_bonita.find_all('span', {'class': 'andes-money-amount__fraction'})
                list_condition_promo = sopa_bonita.find_all('span', {'class': 'andes-money-amount__discount'})
                img_tags = sopa_bonita.find_all('img', class_='ui-pdp-image ui-pdp-gallery__figure__image')
                img_srcs = [img.get('src') for img in img_tags]
                list_parcela_green = sopa_bonita.find_all('p', {'class': 'ui-pdp-color--GREEN ui-pdp-size--MEDIUM ui-pdp-family--REGULAR'})
                list_parcela_black = sopa_bonita.find_all('p', {'class': 'ui-pdp-color--BLACK ui-pdp-size--MEDIUM ui-pdp-family--REGULAR'})
                list_parcelado = list_parcela_green + list_parcela_black


                for titulo, moeda, preco_promo,condition_promo, img, parcelado in zip(list_titulo, list_moeda, list_preco_promo,list_condition_promo, img_srcs,list_parcelado):

                    titulo_text = titulo.text.strip()
                    moeda = moeda.text.strip()
                    preco_promo_text = preco_promo.text.strip().replace('\xa0','').replace('.','').replace(',','.')
                    condition_promo_text = condition_promo.text.strip()
                    imagem = str(img).replace('[', '').replace(']', '')
                    parcelado_text = parcelado.text.strip()    

                    titulo_text = titulo_text.replace('\n', '').strip()
                    preco_promo_text = preco_promo_text.replace('\n', '').strip()
                    condition_promo_text = condition_promo_text.replace('\n', '').strip()
                    parcelado_text = parcelado_text.replace('\n', '').strip()


                    list_todos.append({
                        'titulo': titulo_text,
                        'moeda': moeda,
                        'preco_promo': preco_promo_text,
                        'condition_promo': condition_promo_text,
                        'parcelado': parcelado_text,
                        'imagem': imagem 


                    })

        output_folder = "/home/fececa/airflow/dags/nintendo/data/bronze/ml"
        file_name_sem_extensao = filename.replace(".txt", "")
        output_file_name = f"{file_name_sem_extensao}.json"
        output_file_path = os.path.join(output_folder, output_file_name)

        os.makedirs(output_folder, exist_ok=True)

        with open(output_file_path, 'w', encoding="utf-8") as json_file:
            json.dump(list_todos, json_file, ensure_ascii=False, indent=4)
