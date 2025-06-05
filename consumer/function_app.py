import azure.functions as func
import logging
import datetime

app = func.FunctionApp()

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="consumidor_request",
                               connection="eventhubspacenintendo_RootManageSharedAccessKey_EVENTHUB") 

@app.blob_output(arg_name="outputblob", path="nintendo/inbound/kabum_{sys.utcnow}.json", connection="AzureStorageConnection")

def consumidor(azeventhub: func.EventHubEvent, outputblob: func.Out[str]):
    logging.info('Python EventHub trigger processed an event: %s',
                azeventhub.get_body().decode('utf-8'))


    # Assumindo que os dados do Event Hub são strings JSON
    event_data_str = azeventhub.get_body().decode('utf-8')
    logging.info(f'Received event: {event_data_str}')

    try:
        # Opcional: Processar os dados se necessário
        # event_data_json = json.loads(event_data_str)
        # logging.info(f'Parsed event: {event_data_json}')

        # Criar um nome de arquivo único para o blob
        # Exemplo: 'dados_api/2025/06/04/1330_uuid.json'
        # Você pode ajustar o formato do nome do arquivo conforme sua necessidade.
        current_utc_time = datetime.datetime.now()
        file_name = f"dados_kabum_/{current_utc_time.strftime('%Y-%m-%d-%H-%M-%S')}_{azeventhub.sequence_number}.json"

        # Escrever os dados no Blob Storage
        # O binding de saída 'outputblob' cuidará da escrita.
        outputblob.set(event_data_str)
        logging.info(f'Data sent to blob: {file_name}')

    except Exception as e:
        logging.error(f"Error processing event: {e}")
