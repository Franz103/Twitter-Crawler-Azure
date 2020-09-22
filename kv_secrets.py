import os
import cmd
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

def get_bearer_token():
    keyVaultName = os.environ["KEY_VAULT_NAME"]
    KVUri = "https://{}.vault.azure.net".format(keyVaultName)
    secretName = "BEARER-TOKEN"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    print("Retrieving your secret from {}.".format(keyVaultName))
    retrieved_secret = client.get_secret(secretName).value
    print("Secret retrieval done.")
    return retrieved_secret

def get_lake_credentials():
    name = os.environ["STORAGE_ACCOUNT_NAME"]
    key = os.environ["STORAGE_ACCOUNT_KEY"]
    return name, key
