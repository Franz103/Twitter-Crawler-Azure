import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
import datetime, time
from pytz import timezone
import kv_secrets



def main(functions,urls):
    storage_account_name, storage_account_key = kv_secrets.get_lake_credentials()
    establish_connection(storage_account_name, storage_account_key)
    file_system_name = "raw"
    berlin = timezone('Europe/Berlin')
    time = datetime.datetime.now(berlin)
    #create_file_system(file_system_name)
    #The function is always called with the url of the same index
    for f in functions:    
        local_file, category = f(urls[functions.index(f)])
        current_directory = get_current_directory(time, category)
        print(current_directory)
        try:
            create_directory(service_client.get_file_system_client(file_system=file_system_name),current_directory)
        except Exception as e:
            print(e)
        file_name = local_file
        upload_file_to_directory(file_system_name, current_directory, file_name, local_file)
        os.remove(local_file)

def get_current_directory(date, category):
    directory = date.strftime("controlling/hubspot/"+category+ "/%Y/%m/%d")
    return directory

def establish_connection(storage_account_name, storage_account_key):
    try:
        global service_client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=storage_account_key)
        print("connection established to {}".format(storage_account_name))
    except Exception as e:
        print(e)

def create_file_system(file_system_name):
    try:
        global file_system_client
        file_system_client = service_client.create_file_system(file_system=file_system_name)
        print("created file system {}".format(file_system_name))
    except Exception as e:
        print(e)

def create_directory(file_system_client,dir_name):
    try:
        directory_client = file_system_client.create_directory(dir_name)
        print("created directory {}".format(dir_name))
        return directory_client
    except Exception as e:
        print(e)

def upload_file_to_directory(file_system_name, directory, file_name, local_file):
    try:

        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        directory_client = file_system_client.get_directory_client(directory)
        file_client = directory_client.create_file(file_name)
        local_file = open(local_file,'r')
        file_contents = local_file.read()
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
        print("uploaded file {}".format(local_file))
        os.system("rm {}".format(local_file))
    except Exception as e:
        print(e)





if __name__ == "__main__":
    functions = [collect.get_all_companies, collect.get_all_deals,collect.get_all_contacts, collect.get_all_owners] #functions to get all the data
    #get_all_urls = [collect.get_all_company_url, collect.get_all_deals_url,collect.get_all_contacts_url, collect.get_all_owners_url]
    get_recent_urls = [collect.get_recent_company_url, collect.get_recent_deals_url, collect.get_recent_contacts_url, collect.get_all_owners_url]
    #main([collect.get_all_deals],[collect.get_all_deals_url])
    while True:
        main(functions, get_recent_urls)
        time.sleep(7*86400)
