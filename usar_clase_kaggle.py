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

from pathlib import Path
import os


#%%

''' Crear una instancia de la clase (objeto) '''
##llamado de construccion de inicializacion
    
extractor = data_extractor()
#save extrac + posicion 
    
extractor.path = dir_root
    
    
##intancia para transformacion
transform=data_transformer()


def extraccion(posicion):
    #%%
    
    '''Autenticación en el api de Kaggle'''
    
    if (posicion == 0):
    
        path_auth = str(p(extractor.path) / 'kaggle')
        extractor.set_kaggle_api(path_auth)
        #NOTA SALIDA AL USUARIO QUE SE AUTENTICO A TRAVES DE LOG
        
        
        write(1,0)
        posicion = posicion + 1
        
    
        #%%
        ''' Listar datasets'''
        extractor.list_dataset_kaggle('youtube')
        extractor.show_kaggle_datasets()
            
    #%%
    
    if(posicion == 1):
    
        ''' Descargar todos los archivos de un dataset alojado kaggle '''
        
        path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
        extractor.get_data_from_kaggle_d(path_data,'datasnaek/youtube-new')
        
        
        #%%
        ''' Listar archivos según el tipo'''
        path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
        extractor.get_lst_files(path_data,'csv')
        print('{}Instancia kaggle - archivos formato {}:'.format(os.linesep,'csv'))
        extractor.muestra_archivos()
        
        write(1,1)
        posicion = posicion + 1
        
    
    #%%
    
    if (posicion == 2):
        
        ''' UNIFICACION DE REGISTROS, TODOS LOS PAISES ALMACENADOS EN UN UNNICA DATAFRAME, IDENTIFICADOS POR PAIS'''
        
        #---------------------------------------------------------------------------------------------------------------
        df = pd.DataFrame()
        
        print('Unificando Datasets.....')
        for i in range(0,10):
            extractor.get_data_csv(extractor.lst_files[i])
            df1 = extractor.data
            
            
            df1['pais'] = i

            df = pd.concat([df, df1],ignore_index=True)
          
        #-------------------------------------------------------------------------------------------------------------
        
        #%%
        #Mostrar datos para información
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
        #Ejemplo: Cargar datos "Canada" a memoria, i.e. CAvideos.csv
        extractor.get_data_csv(extractor.lst_files[0])
        extractor.data.drop_duplicates(subset='video_id', keep='last', inplace=True)
        
        '''
        
        #%%
        ''' Mostrar datos para información'''
        print(extractor.data)
        
        
        write(1,2)
        
    
    #Continuacion del proceso de ETL
    transformacion(0)



def transformacion(posicion):
    
    
    #%%
    
    if (posicion == 0):  
        
        print("{}Nombres de las columnas:".format(os.linesep))
        [print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]
        
        write(2,0)
        posicion = posicion + 1
        
    
    #%% 
    
    if (posicion == 1):
        ##ELIMINACION DE COLOMUNAS IRRELEVANTES
        transform.drop_columns(['thumbnail_link', 'comments_disabled', 'ratings_disabled', 'video_error_or_removed','description'])
        print("{}Nombres de las columnas:".format(os.linesep))
        ##IMPRESION DE LISTA POR COMPRENSION
        [print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]
        print("")
        
        write(2,1)
        posicion = posicion + 1
    
    #%%
    
    if (posicion == 2):
        ##nomaliacion de la fecha
        transform.normalize_trending_date('trending_date')
        print('Normalizacion de la fecha:')
        print(transform.data.trending_date)
        print("")
        print("")
        
        write(2,2)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 3):
        ##Creacion de indice para fecha
        transform.set_index()
        print(transform.data.index)
        print("")
        write(2,3)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 4):
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
        
        write(2,4)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 5):
        ##Unificar fechas
        ##separacion de la fecha y hora de publicación del vídeo en columnas separadas
        print('Normalizacion de la fecha de publicacion y unificacion con fecha de tendencia')
        print (transform.data.publish_time)
        print("")
        transform.normalize_publish_date('publish_time')
        print(transform.data)
        print("")
        
        write(2,5)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 6):
        ##Cambio de tipo de dato
        print('Cambio tipo de dato')
        transform.change_data_type('likes')
        print(transform.data.likes)
        print("")
        write(2,6)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 7):
        ##Decodificacion de datos
        print('Decodificacion de datos')
        transform.decode('tags')
        print(transform.data.tags)
        print("")
        write(2,7)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 8):
        ##Obtener maximo valor 
        print('Obtener maximo valor')
        transform.max_('likes')
        print(transform.data.likes)
        print("")
        write(2,8)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 9):
        ##Obtener minimo valor 
        print('Obtener  minimo valor')
        transform.min_('likes')
        print(transform.data.likes)
        print("")
        write(2,9)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 10):
        ##(calcular el tiempo para que un vídeo pase de estar publicado a ser tendencia 
        print('calculo del tiempo para que un vídeo pase de estar publicado a ser tendencia')
        transform.time_to_hit('trending_date','publish_date')
        print("")
        write(2,10)
        posicion = posicion + 1
    
    #%%
    
    if (posicion == 11):
        #Representacion Tags semiestructoado o no 
        print("TAGS ESTRUCTURADOS")
        #print(transform.data.tags)
        transform.structured_data('tags')
        write(2,11)
    
    carga(0)
    
#%%
def carga(posicion):
    
    if (posicion == 0):
                                             
        output_data = str(p(extractor.path) / 'output' / 'youtube-transformed.csv')
        print (output_data)
        transform.save_data_csv(output_data)
        
        write(3,0)
        print("Finalizacion proceso ETL")
        
#---------------------------------------------------------------------------------------


def write(metodo,posicion):
    
    fileName = open("C:/Users/Juan Esteban Puyo/Documents/2021-2/DATA ANALYTICS/TransformNLoading/test.txt", "a") 
    fileName.write(os.linesep+str(metodo)+","+str(posicion))
    fileName.close()



fileName = r"C:/Users/Juan Esteban Puyo/Documents/2021-2/DATA ANALYTICS/TransformNLoading/test.txt"
fileObj = Path(fileName)



if (fileObj.is_file() == True):
    
    
    '''
    extraer la ubicacion donde quedo
    
    '''
    
    with open("C:/Users/Juan Esteban Puyo/Documents/2021-2/DATA ANALYTICS/TransformNLoading/test.txt","r") as archivo:
        file_lines = archivo.readlines ()
        archivo.close()
        ultima_registro = file_lines [len (file_lines) -1]
        print("El ultimo registro es" + ultima_registro)
        
        x = ultima_registro.split(",")
        print(x)
        
        metodo = int(x[0])
        posicion = int(x[1])
        
        if(metodo == 1):
            print("Continuacion proceso de extraccion...")
            print("Paso:" , posicion)
            extraccion(posicion)
        elif(metodo == 2):
            print("Continuacion proceso de transformacion...")
            print("Paso:" , posicion)
            transformacion(posicion)
        else:
            print("Continuacion proceso de carga...")
            print("Paso:" , posicion)
            carga(posicion)
    
    
elif(fileObj.is_file() == False):
    write(0,0)
    extraccion(0)
    


