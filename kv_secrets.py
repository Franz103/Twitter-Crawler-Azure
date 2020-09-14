import os
import cmd
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

get_bearer_token():
    keyVaultName = os.environ["KEY_VAULT_NAME"]
    KVUri = "https://{}.vault.azure.net".format(keyVaultName)
    secretName = "BEARER-TOKEN"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    print("Retrieving your secret from {}.".format(keyVaultName))
    retrieved_secret = client.get_secret(secretName)
    print("Your secret is '{}'.".format(retrieved_secret.value))
    print(" done.")
    return retrieved_secret
