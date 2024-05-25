import logging
import azure.functions as func
import csv
from io import StringIO
from azure.storage.blob import BlobServiceClient
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Get form data
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Invalid input",
            status_code=400
        )

    name = req_body.get('name')
    email = req_body.get('email')
    message = req_body.get('message')

    if not name or not email or not message:
        return func.HttpResponse(
            "Please pass name, email, and message in the request body",
            status_code=400
        )

    # Azure Blob Storage connection details
    connect_str = os.getenv('AzureWebJobsStorage')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = "formdata"
    blob_name = "form_data.csv"

    # Create container if not exists
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()

    # Download existing blob content if exists
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_data = ""
    if blob_client.exists():
        downloader = blob_client.download_blob()
        blob_data = downloader.readall().decode()

    # Append new data
    output = StringIO()
    fieldnames = ['name', 'email', 'message']
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    
    if not blob_data:
        writer.writeheader()
    else:
        output.write(blob_data)

    writer.writerow({'name': name, 'email': email, 'message': message})

    # Upload the new blob content
    blob_client.upload_blob(output.getvalue(), overwrite=True)

    return func.HttpResponse(f"Data received: Name={name}, Email={email}, Message={message}", status_code=200)
