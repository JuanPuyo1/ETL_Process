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

''' Proceso de extración'''
''' Este porceso hace uso del api Kaggle para descarga los dato del los videos en 
    tendencia en Canada, Mexico, Francia, Reino Unido entre otros a travez de:
    - Conexion con la api Kaggle
    - Descarga de los dataset en formato CSV
    - unificación de todos los datasets en uno unico
    '''
''' Esta cuenta con el control de reinicio en caso de detener el proceso'''

def extraccion(posicion):
    #%%
    
    if (posicion == 0):
        ##Busca el archivo Kaggle.json con los datos de autenticación para usar la api
        path_auth = str(p(extractor.path) / 'kaggle')
        ##Envia la ubicación del archivo
        extractor.set_kaggle_api(path_auth)
        #NOTA SALIDA AL USUARIO QUE SE AUTENTICO A TRAVES DE LOG
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(1,0)
        posicion = posicion + 1
        
    
        #%%
        ''' Listar datasets'''
        ##Busca los dataset relacionados con youtube
        extractor.list_dataset_kaggle('youtube')
        ##Muestra los dataset encontrados
        extractor.show_kaggle_datasets()
            
    #%%
    
    if(posicion == 1):
    
        ''' Descargar todos los archivos de un dataset alojado kaggle '''
        path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
        extractor.get_data_from_kaggle_d(path_data,'datasnaek/youtube-new')
        
        #%%
        ''' Listar archivos según el tipo'''
        ##Especificamente solo se trabajaran con los que esten en formato .csv
        path_data = str(p(extractor.path) / 'Dataset' / 'YouTube-New')
        extractor.get_lst_files(path_data,'csv')
        #Muestra solo los archicos .csv
        print('{}Instancia kaggle - archivos formato {}:'.format(os.linesep,'csv'))
        extractor.muestra_archivos()
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(1,1)
        posicion = posicion + 1
        
    
    #%%
    
    if (posicion == 2):
        
        ''' UNIFICACION DE REGISTROS, TODOS LOS PAISES ALMACENADOS EN UN UNICO 
            DATAFRAME, IDENTIFICADOS POR PAIS'''
        #---------------------------------------------------------------------------------------------------------------
        ##Crea el DataFrame que almacenara la informacion de todos los paises
        df = pd.DataFrame()
        ''' Unificando todos los Datasets'''
        print('Unificando Datasets.....')
        for i in range(0,10):
            ##Extrae el dataset segun el indice i
            extractor.get_data_csv(extractor.lst_files[i])
            ##Guarda el dataset en un DataFrame
            df1 = extractor.data
            ##Crea la columna de pais y la llena con el identificador de ese pais
            df1['pais'] = i
            ##Concatenamos el DataFrame df con el df1
            df = pd.concat([df, df1],ignore_index=True)
          
        #-------------------------------------------------------------------------------------------------------------
        
        #%%
        #Mostrar datos para información
        ## ALMACENADO EN UN DATAFRAME
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
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(1,2)
        
    
    #Continuacion del proceso de ETL
    transformacion(0)

''' Proceso de transfomación'''
''' Este porceso hacer cambios al dataset y a los formatos de los datos como:
    - Estandarizar el manejo de fechas al estandar colombiano
    - Eliminar los datos que no son relevantes
    - Gestinar la codificacion de los texto a UTF8
    - Cambio de tipos de datos
    - Optencion de información relevate ("Maximo numero de likes, Minimo valor de likes","Tiempo en que un video se volvio viral")
    - Gestinar la decodificacion de los texto a UTF8
    '''
''' Esta cuenta con el control de reinicio en caso de detener el proceso'''
def transformacion(posicion):
    
    
    #%%
    #Impresion de todas las columnas que tiene el DataFrame transform
    if (posicion == 0):  
        
        print("{}Nombres de las columnas:".format(os.linesep))
        [print ("\'{}\'".format(fn)) for fn in transform.data.columns.values]
        
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
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
        
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(2,1)
        posicion = posicion + 1
    
    #%%
    
    if (posicion == 2):
        ##normaliacion de la fecha en que el video se hizo viral
        transform.normalize_trending_date('trending_date')
        ##Muestra el nuevo formato
        print('Normalizacion de la fecha:')
        print(transform.data.trending_date)
        print("")
        print("")
        
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(2,2)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 3):
        ##Creacion de indice para fecha
        transform.set_index()
        ##Muestra el nuevo indice
        print(transform.data.index)
        print("")
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(2,3)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 4):
        ##Gestionar Codificacion del titilo del video a UTF8
        print('Codificacion de titulo UTF8')
        transform.encode('title')
        ##Muestra la codificación
        print (transform.data.title)
        print("")
        ##Gestionar Codificacion del titilo del canal a UTF8
        print('Codificacion del canal UTF8')
        transform.encode('channel_title')
        ##Muestra la codificación
        print (transform.data.channel_title)
        print("")
        ##Gestionar Codificacion de los tags a UTF8
        print('Codificacion de tags UTF8')
        transform.encode('tags')
        ##Muestra la codificación
        print (transform.data.tags)
        print("")
        #print (transform.data.tags)
        
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(2,4)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 5):
        ##Unificar fechas
        ##separacion de la fecha y hora de publicación del vídeo en columnas separadas
        print('Normalizacion de la fecha de publicacion y unificacion con fecha de tendencia')
        ##Muestra las dos nuevas columnas
        print (transform.data.publish_time)
        print("")
        transform.normalize_publish_date('publish_time')
        print(transform.data)
        print("")
        
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(2,5)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 6):
        ##Cambio de tipo de dato
        print('Cambio tipo de dato')
        transform.change_data_type('likes')
        ##Muestra los datos en su nuevo tipo
        print(transform.data.likes)
        print("")
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(2,6)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 7):
        ##Decodificacion de datos que esta en UTF8
        print('Decodificacion de datos')
        ##Decodifica los tags
        transform.decode('tags')
        ##Muestra los tags decodificados
        print(transform.data.tags)
        print("")
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(2,7)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 8):
        ##Obtener maximo valor de likes alcanzado por un video 
        print('Obtener maximo valor')
        ##Obtener el valor maximo
        transform.max_('likes')
        print(transform.data.likes)
        print("")
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(2,8)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 9):
        ##Obtener minimo valor de likes alcanzado por un video
        print('Obtener  minimo valor')
        ##Obtener el valor minimo
        transform.min_('likes')
        print(transform.data.likes)
        print("")
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(2,9)
        posicion = posicion + 1
    
    #%% 
    
    if (posicion == 10):
        ##(calcular el tiempo para que un vídeo pase de estar 
        ##publicado a ser tendencia 
        print('calculo del tiempo para que un vídeo pase de estar publicado a ser tendencia')
        transform.time_to_hit('trending_date','publish_date')
        print("")
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(2,10)
        posicion = posicion + 1
    
    #%%
    
    if (posicion == 11):
        #Representacion Tags en datos semiestructoados
        print("TAGS ESTRUCTURADOS")
        #print(transform.data.tags)
        transform.structured_data('tags')
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(2,11)
    
    carga(0)
    
#%%

''' Proceso de carga'''
''' Este proceso se encarga de subir el dataset que paso por el proceso de
    transformacion a un repository de la siguiente manera:
    - Crea el path del archivo final
    - Verifica el path
    - Guarda el DataFrame en un archivo final
    '''
''' Esta cuenta con el control de reinicio en caso de detener el proceso'''

def carga(posicion):
    
    if (posicion == 0):
        
        #Se indica la ubicacion donde se almacenara el DataFrame
        output_data = str(p(extractor.path) / 'output' / 'youtube-transformed.csv')
        ##Muestra la ruta
        print (output_data)
        ##Guarda el DataFrame Final
        transform.save_data_csv(output_data)
        
        ##Guarda es estado que se acaba de ejecutar el en archivo test
        write(3,0)
        print("Finalizacion proceso ETL")
        
#---------------------------------------------------------------------------------------


def write(metodo,posicion):
    
    ##Indica en que archivo quiere escribir
    fileName = open("C:/Users/Juan Esteban Puyo/Documents/2021-2/DATA ANALYTICS/TransformNLoading/test.txt", "a") 
    ##Escribe sobre el archivo el estado que se envio
    fileName.write(os.linesep+str(metodo)+","+str(posicion))
    ##Cierra el archivo
    fileName.close()


#---------------------------------------------------------------------------------------


''' -------------------Inicio del proceso de ETL-----------------------------------'''

## Path del archivo con la logica de reinicio
fileName = r"C:/Users/Juan Esteban Puyo/Documents/2021-2/DATA ANALYTICS/TransformNLoading/test.txt"
fileObj = Path(fileName)

## Validacion de que existe el archivo para el control de reinicio

if (fileObj.is_file() == True):
    
    
    '''
    extraer la ubicacion donde quedo
    
    '''
    
    with open("C:/Users/Juan Esteban Puyo/Documents/2021-2/DATA ANALYTICS/TransformNLoading/test.txt","r") as archivo:
        file_lines = archivo.readlines ()
        archivo.close()
        ##Revisa la ultima linea del archivo
        ultima_registro = file_lines [len (file_lines) -1]
        ##Imprime la ultima linea
        print("El ultimo registro es" + ultima_registro)
        ##Separa la ultima linea por comas
        x = ultima_registro.split(",")
        print(x)
        ##Extrae el ultimo metodo en el que se quedo el proceso
        metodo = int(x[0])
        ##Extrae el ultimo paso en el que se quedo el proceso
        posicion = int(x[1])
        
        ##Entra a el proceso de estración
        if(metodo == 1):
            print("Continuacion proceso de extraccion...")
            print("Paso:" , posicion)
            extraccion(posicion)
        ##Entra a el proceso de transformación
        elif(metodo == 2):
            print("Continuacion proceso de transformacion...")
            print("Paso:" , posicion)
            transformacion(posicion)
        ##Entra al proceso de carga
        else:
            print("Continuacion proceso de carga...")
            print("Paso:" , posicion)
            carga(posicion)
##Si el archivo no existe inicia la captura de los estado y inicia la Extración    
elif(fileObj.is_file() == False):
    write(0,0)
    extraccion(0)
    
