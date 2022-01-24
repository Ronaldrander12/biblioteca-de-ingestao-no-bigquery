import os
import yaml
from google.cloud import secretmanager
import json
from google.oauth2 import service_account
from pathlib import Path

class Credentials():
    def __init__(self) -> None:
        self.client_secret = secretmanager.SecretManageServiceClient()
        self.path_cred = '--Caminho do arquivo de credenciais--'

    def set_environment_keys(self, key_type='main_key', path_key=False):
        if key_type=='main_key':
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= self.path_cred

    def access_secret_version(self, secret_id, version_id="latest"):
        client = secretmanager.SecretManagerServiceClient()
        gcp_project_id = self.client_secret.project
        name = f"projects/{gcp_project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(name=name)

        return response.payload.data.decode('UTF-8')

    def service_account_credentials(self, secret_id):
        json_acct_info = json.loads(self.access_secret_version(secret_id))

        return service_account.Credentials.from_service_account_info(json_acct_info)
