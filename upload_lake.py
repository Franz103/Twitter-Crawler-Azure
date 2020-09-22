import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient

from pytz import timezone
import kv_secrets
import ntpath



def upload(local_file):
    storage_account_name, storage_account_key = kv_secrets.get_lake_credentials()
    service_client = establish_connection(storage_account_name, storage_account_key)
    file_system_name = "raw"
    directory = get_current_directory(local_file)
    #file_system_client = create_file_system(service_client, file_system_name)
    #The function is always called with the url of the same index
    try:
        create_directory(service_client.get_file_system_client(file_system=file_system_name),directory)
    except Exception as e:
        print(e)
    upload_file_to_directory(service_client, file_system_name, directory, local_file, local_file)
    os.remove(local_file)

def get_current_directory(local_file):
    splitted = ntpath.basename(local_file).split("-")
    month = splitted[1]
    year = splitted[0]
    directory = "socialmedia/twitter/streaming/" + year + "/" + month
    return directory

def establish_connection(storage_account_name, storage_account_key):
    try:
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=storage_account_key)
        print("connection established to {}".format(storage_account_name))
        return service_client
    except Exception as e:
        print(e)

def create_file_system(service_client, file_system_name):
    try:
        file_system_client = service_client.create_file_system(file_system=file_system_name)
        print("created file system {}".format(file_system_name))
        return file_system_client
    except Exception as e:
        print(e)

def create_directory(file_system_client,dir_name):
    try:
        directory_client = file_system_client.create_directory(dir_name)
        print("created directory {}".format(dir_name))
        return directory_client
    except Exception as e:
        print(e)

def upload_file_to_directory(service_client,file_system_name, directory, file_name, local_file):
    try:
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        directory_client = file_system_client.get_directory_client(directory)
        file_client = directory_client.create_file(ntpath.basename(file_name))
        local_file = open(local_file,'r')
        file_contents = local_file.read()
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
        print("uploaded file {}".format(local_file))
        os.system("rm {}".format(local_file))
    except Exception as e:
        print(e)