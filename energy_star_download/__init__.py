import logging
import os
import requests
import datetime
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(mytimer: func.TimerRequest) -> None:
    json_url = os.getenv("BASE_URI")
    container_name = os.getenv("BLOB_CONTAINER_NAME")
    connection_string = os.getenv("AzureWebJobsStorage")

#    logging.info(f"Downloading from {json_url}...")
#    response = requests.get(json_url)
#    response.raise_for_status()
    
    file_path = os.path.join(os.path.dirname(__file__), "endpoints.json")
    with open(file_path) as f:
        config = json.load(f)
    
    for equipment_type in config:
        logging.info(f"Starting extract for {equipment_type}")
        for item in config[equipment_type]:
            logging.info(f"Starting extract for {item}")

            item_uri= json_url + item['api_id']
            logging.info(f'Getting item from {item_uri}')
            data = requests.get(item_uri)
            data.raise_for_status()

            #logging.info(response.json())

            logging.info("Uploading to Azure Blob Storage...")
            
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)

            container_client = blob_service_client.get_container_client(container_name)

            if not container_client.exists():
                container_client.create_container()

            #archive blob first
            item_name = item['type'].lower().replace(' ', '_')
            timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            blob_name = f"{equipment_type}_{item_name}_{timestamp}.json"
            archive_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            archive_blob_client.upload_blob(data.content, overwrite=True)

            logging.info(f"Uploaded {blob_name} successfully.")

            #Now the latest version for the API
            latest_blob_name = f"{equipment_type}_{item_name}_latest.json"
            latest_blob_client = blob_service_client.get_blob_client(container=container_name, blob=latest_blob_name)
            latest_blob_client.upload_blob(data.content, overwrite=True)

            logging.info(f"Uploaded {blob_name} successfully.")
            
            
