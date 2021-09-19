try:
    import os
    from pathlib import Path as p
    import sys
except Exception as exc:
            print('Module(s) {} are missing.:'.format(str(exc)))


dir_root = p(__file__).parents[1]
sys.path.append(str(p(dir_root) /'source' / 'clases'))

from cls_extract_data_mf import extract_data_mf as get_data
from cls_azure_blob_storage import AzureBlob_container_bs_ops as new_container


#%%
''' 
Asignar valores para cada atributo de la clase AzureBlob_container_bs_ops

'''

# Cadena de conexión a Azure Storage Blob
cnx = "DefaultEndpointsProtocol=https;AccountName=daucentralstorage;AccountKey=park5MkZmh8ehB2Fx1edVpxzSu2YrFCSM3m4Nfuqs1l6d/PYkoHg7zRZHd9BYN8m7c5I/iDpYNY9SBIr+SEYCw==;EndpointSuffix=core.windows.net"

'''
El directorio de carga de archivos al datalake es el mismo de salida de
la transformación
'''

dpath = os.path.abspath(os.path.join(__file__, '..','..'))
folder_upload = str(p(dpath) / 'output' )
cmd_folder = folder_upload

#%%
''' Asignar valores a los atributos de la clase en la instancia '''
loader_blob = new_container(cnx = cnx)
loader_blob.set_connection()

#%%
''' Listar todos los contenedores de una cuenta '''
loader_blob.list_container(Show=True)


#%%
''' Crear un nuevo contenedor '''
loader_blob.create_container('micontenedor')


#%%
''' Listar todos los blobs de un contenedor '''
loader_blob.list_files_container('micontenedor',Show=True)

#%%
''' Eliminar un contenedor y sus contenido '''
loader_blob.delete_container('micontenedorbd')

#%%
''' Inicializar la carga de datos '''
extractor = get_data()

#%%
''' Lista los archivos descargados '''
extractor.get_lst_files(folder_upload,'csv')

print('{}Instancia CSV:'.format(os.linesep))
extractor.muestra_archivos()
#%%

# Cargar el archivo transformado al datalake de Azure (BLOB)
loader_blob.upload_file('micontenedor',folder_upload,extractor.lst_files[0],'csv')
