import json
from azure.storage.blob import BlobServiceClient


class SystemUtils:

    @staticmethod
    def upload_to_blob_storage(market, changed_date, conn_str, container_name, data_json):

        filename = f"{market}-{changed_date}.json"
        blob_service_client = BlobServiceClient.from_connection_string(conn_str)
        container_client = blob_service_client.get_container_client(container_name)

        try:
            blob_clinet = container_client.get_blob_client(filename)
            data_json = json.dumps(data_json, indent=4, sort_keys=True, ensure_ascii=False)
            blob_clinet.upload_blob(data_json, blob_type='BlockBlob')

        except json.JSONDecodeError as e:
            print(f"JSON encoding/decoding error: {str(e)}. Please check the input data structure.")

        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
