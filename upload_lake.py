import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient

from pytz import timezone
import kv_secrets
import ntpath
import pandas as pd
from io import BytesIO

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
    day = splitted[3].split(".")[0]
    month = splitted[2]
    year = splitted[1]
    type = splitted[0]
    directory = "socialmedia/twitter/streaming/new/"+ type + "/" + year + "/" + month + "/" + day
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
        local_file_wrapper = open(local_file,'r')
        file_contents = local_file_wrapper.read()
        #file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        #file_client.flush_data(len(file_contents))
        file_client.upload_data(file_contents, overwrite=True)
        print("uploaded file {}".format(local_file))
    except Exception as e:
        print(e)
        
def download(local_file):
    try:
        storage_account_name, storage_account_key = kv_secrets.get_lake_credentials()
        service_client = establish_connection(storage_account_name, storage_account_key)
        file_system_name = "raw"
        directory = get_current_directory(local_file)
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        file = directory + "/" + local_file
        file_client = file_system_client.get_file_client(file)
        stream = file_client.download_file()
        if len(stream.readall()) > 0:
            df = pd.read_csv(BytesIO(stream.readall()), delimiter=";", header=0)
            print("downloaded file {}".format(local_file))
        else:
            df = None
    except Exception as e:
        df = None
    return df