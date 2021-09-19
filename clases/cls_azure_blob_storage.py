# -*- coding: utf-8 -*-
__author__ = "Juan Puyo"
"


try:
    import os
    from multiprocessing.pool import ThreadPool
    from azure.storage.blob import BlobServiceClient

except Exception as exc:
            print('Problema de módulos: {}.'.format(str(exc)))


DEFAULT_ENCODING = 'UTF-8-SIG'
#%%
class AzureBlobFileDownloader(object):
    
    def __init__(self,cnx = None,cmd_folder = None,container = None):
      self.LOCAL_BLOB_PATH = cmd_folder
      self.MY_CONNECTION_STRING = cnx
      self.MY_BLOB_CONTAINER = container
      self.nfiles = 10
      
    def set_connection(self):
      # Initialize the connection to Azure storage account
      self.blob_service_client =  BlobServiceClient.from_connection_string(self.MY_CONNECTION_STRING)
      self.my_container = self.blob_service_client.get_container_client(self.MY_BLOB_CONTAINER)
    
    def list_files_container(self):
        self.my_blobs = self.my_container.list_blobs()
    
    def download_all_blobs_in_container(self):
      # get a list of blobs
      my_blobs = self.my_container.list_blobs()
      result = self.run(my_blobs)
      print(result)
   
    def run(self,blobs):
      # Download several files at a time!
      with ThreadPool(processes=int(self.nfiles)) as pool:
       return pool.map(self.save_blob_locally, blobs)
   
    def save_blob_locally(self,blob):
      file_name = blob.name
      print(file_name)
      bytes = self.my_container.get_blob_client(blob).download_blob().readall()
   
      # Get full path to the file
      download_file_path = os.path.join(self.LOCAL_BLOB_PATH, file_name)
      # for nested blobs, create local path as well!
      os.makedirs(os.path.dirname(download_file_path), exist_ok=True)
   
      with open(download_file_path, "wb") as file:
        file.write(bytes)
      return file_name

class AzureBlobFileDownloader_lineal(object):
    def __init__(self,cnx = None,cmd_folder = None,container = None):
      self.LOCAL_BLOB_PATH = cmd_folder
      self.MY_CONNECTION_STRING = cnx
      self.MY_BLOB_CONTAINER = container
        
    def set_connection(self):
      # Initialize the connection to Azure storage account
      self.blob_service_client =  BlobServiceClient.from_connection_string(self.MY_CONNECTION_STRING)
      self.my_container = self.blob_service_client.get_container_client(self.MY_BLOB_CONTAINER)
   
   
    def save_blob(self,file_name,file_content):
      # Get full path to the file
      download_file_path = os.path.join(self.LOCAL_BLOB_PATH, file_name)
   
      # for nested blobs, create local path as well!
      os.makedirs(os.path.dirname(download_file_path), exist_ok=True)
   
      with open(download_file_path, "wb") as file:
        file.write(file_content)
    
    def list_files_container(self):
        self.my_blobs = self.my_container.list_blobs()
    
    def download_all_blobs_in_container(self):
      my_blobs = self.my_container.list_blobs()
      for blob in my_blobs:
        print(blob.name)
        bytes = self.my_container.get_blob_client(blob).download_blob().readall()
        self.save_blob(blob.name, bytes)

class AzureBlob_container_bs_ops(object):
    def __init__(self,cnx = None):
        self.MY_CONNECTION_STRING = cnx
        self.current_blob = None
        self.my_blobs = None
        self.my_containers = None
        
    def set_connection(self):
        self.blob_service_client = BlobServiceClient.from_connection_string(self.MY_CONNECTION_STRING)
        
    def create_container(self,new_container=None):
        if new_container is not None:
            self.my_container = self.blob_service_client.create_container(new_container)
    
    def delete_container(self,del_container=None):
        if del_container is not None:
            self.my_container = self.blob_service_client.get_container_client(del_container)
            self.my_container.delete_container()
    
    def list_container(self,Show = True):
        self.my_containers = self.blob_service_client.list_containers() 
        
        if Show:
            print('List of container:')
            for container in self.my_containers: 
                print (" {}".format(container.name))
        
    def list_files_container(self,container=None,Show = True):
        self.my_container = self.blob_service_client.get_container_client(container)
        self.my_blobs = self.my_container.list_blobs() 
        
        if Show:
            print('List of blobs in {}:'.format(container))
            for blob in self.my_blobs: 
                print (" {}".format(blob.name))
        
    def upload_file(self,container,path_data,file_name,tipo):
        file_name = str(os.path.splitext(os.path.basename(file_name))[0]) + '.' + tipo
        blob_client = self.blob_service_client.get_blob_client(container=container, blob=file_name)
        
        upload_file_path = os.path.join(path_data, file_name)
        
        print('Uploading file - {}'.format(file_name))
        with open(upload_file_path, "rb") as data:
          blob_client.upload_blob(data,overwrite=True)
         