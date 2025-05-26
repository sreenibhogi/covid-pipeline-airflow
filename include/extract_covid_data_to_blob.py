import requests
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from airflow.models import Variable

def get_covid_data_and_upload_to_blob(ti=None):
    url = "https://disease.sh/v3/covid-19/countries/"
    response = requests.get(url)
    
    if response.status_code == 200:
        # Convert response to DataFrame and CSV string
        df = pd.json_normalize(response.json())
        csv_data = df.to_csv(index=False)

        # Define file name
        filename = f"covid_data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"

        # Save locally
        with open(filename, 'w', newline='') as file:
            file.write(csv_data)
        print(f"Saved locally as {filename}")

        # === Azure Blob upload ===
        connection_string = Variable.get("AZURE_CONNECTION_STRING")  # 🔐 Use Airflow Variable for security
        container_name = "coviddata"                    # 🔐 Replace this with container name

        # Initialize BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"coviddata/{filename}")

        # Upload the file
        with open(filename, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        blob_path = f"coviddata/{filename}"
        print(f"Uploaded to Azure Blob: {blob_path}")
        if ti:
            ti.xcom_push(key='blob_path', value=blob_path)
    else:
        print(f"Error: {response.status_code} - {response.text}")

# Call the function
get_covid_data_and_upload_to_blob()
