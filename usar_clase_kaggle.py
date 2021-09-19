# -*- coding: utf-8 -*-
"""
__author__ = "Juan Puyo"
"""
try:
    import os, sys
    from pathlib import Path as p


except Exception as exc:
            print('Module(s) {} are missing.:'.format(str(exc)))

import pandas as pd
pd.set_option("display.max_columns", 6)

##CONSTRUCCION DE DIRECTORIO RAIZ A PARTIR DEL ARCHIVO QUE SE ESTA ESCRIBIENDO
##IDENTIFICACION DE RAIZ
dir_root = p(__file__).parents[1]
sys.path.append(str(p(dir_root) /'source' / 'clases'))

from cls_extract_data_mf import extract_data_mf as data_extractor
from cls_transform_data import transform_data as data_transformer



#%%
''' Crear una instancia de la clase (objeto) '''
##llamado de construccion de inicializacion
extractor = data_extractor()
extractor.path = dir_root

##intancia para transformacion
transform=data_transformer()


#%%
'''Autenticación en el api de Kaggle'''
path_auth = str(p(extractor.path) / 'kaggle')
extractor.set_kaggle_api(path_auth)
#NOTA SALIDA AL USUARIO QUE SE AUTENTICO A TRAVES DE LOG

#%%
''' Listar datasets'''
extractor.list_dataset_kaggle('youtube')
extractor.show_kaggle_datasets()

#%%
''' Descargar todos los archivos de un dataset alojado kaggle '''

path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
extractor.get_data_from_kaggle_d(path_data,'datasnaek/youtube-new')


#%%
''' Listar archivos según el tipo'''
path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
extractor.get_lst_files(path_data,'csv')
print('{}Instancia kaggle - archivos formato {}:'.format(os.linesep,'csv'))
extractor.muestra_archivos()

#%%
''' UNIFICACION DE REGISTROS, TODOS LOS PAISES ALMACENADOS EN UN UNNICA DATAFRAME, IDENTIFICADOS POR PAIS'''

'''
#---------------------------------------------------------------------------------------------------------------
df = pd.DataFrame()

print('Unificando Datasets.....')
for i in range(0,10):
    extractor.get_data_csv(extractor.lst_files[i])
    df1 = extractor.data
    
    
    df1['pais'] = i
    df = pd.concat([df, df1])
  
print(df.pais)
print("")
print('')
#-------------------------------------------------------------------------------------------------------------

#%%
''' '''Mostrar datos para información''''''''
## DALAMACENADO EN UN DATAFRAME
print('DATAFRAME DE TODOS LOS PAISES')
print(df)
print('')
#%%



##pasar una copia del dataset al setdata del transform
transform.set_data(df.copy())
print("{}Nombres de las columnas:".format(os.linesep))
[print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]

'''



#%%
''' Ejemplo: Cargar datos "Canada" a memoria, i.e. CAvideos.csv'''
extractor.get_data_csv(extractor.lst_files[0])
extractor.data.drop_duplicates(subset='video_id', keep='last', inplace=True)


#%%
''' Mostrar datos para información'''
print(extractor.data)

#%%

transform.set_data(extractor.data.copy())
print("{}Nombres de las columnas:".format(os.linesep))
[print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]



#%% 
##ELIMINACION DE COLOMUNAS IRRELEVANTES
transform.drop_columns(['thumbnail_link', 'comments_disabled', 'ratings_disabled', 'video_error_or_removed','description'])
print("{}Nombres de las columnas:".format(os.linesep))
##IMPRESION DE LISTA POR COMPRENSION
[print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]
print("")

#%%
##nomaliacion de la fecha
transform.normalize_trending_date('trending_date')
print('Normalizacion de la fecha:')
print(transform.data.trending_date)
print("")
print("")

#%% 
##Creacion de indice para fecha
transform.set_index()
print(transform.data.index)
print("")

#%% 
##Gestionar Codificacion
print('Codificacion de titulo UTF8')
transform.encode('title')
print (transform.data.title)
print("")

print('Codificacion del canal UTF8')
transform.encode('channel_title')
print (transform.data.channel_title)
print("")

print('Codificacion de tags UTF8')
transform.encode('tags')
print (transform.data.tags)
print("")
#print (transform.data.tags)

#%% 
##Unificar fechas
##separacion de la fecha y hora de publicación del vídeo en columnas separadas
print('Normalizacion de la fecha de publicacion y unificacion con fecha de tendencia')
print (transform.data.publish_time)
print("")
transform.normalize_publish_date('publish_time')
print(transform.data)
print("")

#%% 
##Cambio de tipo de dato
print('Cambio tipo de dato')
transform.change_data_type('likes')
print(transform.data.likes)
print("")

#%% 
##Decodificacion de datos
print('Decodificacion de datos')
transform.decode('tags')
print(transform.data.tags)
print("")

#%% 
##Obtener maximo valor 
print('Obtener maximo valor')
transform.max_('likes')
print(transform.data.likes)
print("")

#%% 
##Obtener minimo valor 
print('Obtener  minimo valor')
transform.min_('likes')
print(transform.data.likes)
print("")

#%% 
##(calcular el tiempo para que un vídeo pase de estar publicado a ser tendencia 
print('calculo del tiempo para que un vídeo pase de estar publicado a ser tendencia')
transform.time_to_hit('trending_date','publish_date')
print("")
#%%
output_data = str(p(extractor.path) / 'output' / 'youtube-transformed.csv')
print (output_data)
transform.save_data_csv(output_data)

#---------------------------------------------------------------------------------------

#Representacion Tags semiestructoado o no 
print("TAGS ESTRUCTURADOS")
print(transform.data.tags)
transform.structured_data('tags')




