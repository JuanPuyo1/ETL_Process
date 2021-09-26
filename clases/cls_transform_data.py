try:
    #from pathlib import Path as p
    import pandas as pd
    from datetime import datetime
    import os
    
    
except Exception as exc:
            print('Module(s) {} are missing.:'.format(str(exc)))

#%%
class transform_data(object):
    def _init_(self, path=None,percent=None):
        self.data = None
        self.status = False
        self.extractor = None
   
#%%  
##  
    def set_data(self, data=None, path=None):
        if path is None:
            self.data=data            
        else:
            try:
                self.data=pd.read_csv(path)
            except Exception as exc:
                self.show_error(exc)
            
#%%
##Metodo 0. Elimimar columnas
    def drop_columns(self, col_list):
        if len(col_list)>0:
            ##Lllamado al metodo drop del dataframe
               self.data=self.data.drop(columns=col_list)
               print("columnas borradas")

#%%    

##Metodo 1. Creacion de indices
    def set_index(self):
        try:
            self.data = self.data.rename_axis('index').reset_index()
        except Exception as exc:
            self.show_error(exc)

#%%    

##Metodo 2. Codificacion
    def encode(self,column_name):
        try:
            self.data[column_name] = self.data[column_name].str.encode(encoding = 'utf8')
        except Exception as exc:
            self.show_error(exc)
    
#%%    

##Metodo 2.1 Decodificacion
    def decode(self,column_name):
        try:
            self.data[column_name] = self.data[column_name].str.decode(encoding = 'utf8')
        except Exception as exc:
            self.show_error(exc)
        
#%%    

##Metodo 3. UnificarFormato y separacon de columnas
    def normalize_publish_date(self,column_name):
        try:
            name = self.data[column_name].str.split('T',expand=True)
            name.columns = ['publish_date', 'publish_TIME']
            self.data = pd.concat([self.data, name], axis=1)
            self.data = self.data.drop([column_name], axis=1)
            self.data['publish_date']=self.data['publish_date'].apply(lambda date:self.format_publish_date_us_to_latam(str(date)))

        except Exception as exc:
            self.show_error(exc)
        
        
#%%    

##Metodo 4. Cambiar tipo de dato
    def change_data_type(self,column_name):
        try:
            self.data[column_name]  = self.data[column_name].astype('float')
        except Exception as exc:
            self.show_error(exc)
        
#%%    

##Metodo 5. Obtener valor maximo
    def max_(self,column_name):   
        try:
            maxs = self.data[column_name].max()
            print("valor maximo de ", column_name)
            print(maxs)

            self.data = self.data.sort_values('likes',ascending=False)
        except Exception as exc:
            self.show_error(exc)
        

#%%    
##Metodo 5.1 Obtener valor minimo
    def min_(self,column_name):
        try:
            min = self.data[column_name].min()

            print("valor minimo de ", column_name)
            print(min)
            self.data = self.data.sort_values('likes',ascending=True)
        except Exception as exc:
            self.show_error(exc)
        
#%%    
##Metodo 6 Operaciones aritmeticas
    def time_to_hit(self,column_name,column_name1):
        try:
            dia1 = self.data[column_name]
            dia2 = self.data[column_name1]
            
            hit = []
            
            for i,j in zip(dia1,dia2):
                i = datetime.strptime(i, '%d/%m/%Y')
                j = datetime.strptime(j, '%d/%m/%Y')
                diferencia = i - j
                hit.append(diferencia.days)
                
            df = pd.DataFrame(hit,columns =['days_to_hit'])
            print(df)
        except Exception as exc:
            self.show_error(exc)
        
#%%    

##Metodo 7. Separacion tags estructarada
    def structured_data(self,column_name):
        try:
            df = self.data[column_name].str.split('|',expand=True)
            print(df)
            
            print("SEMIESTRUCTURADOS")
            js=df.to_json('CAvideos1.json')
            
            print(type(df))
            df1 = df.iloc[0:100] # Primeros 100 datos
            print(df1,type(df1))
            
            res = self.to_xml(df1)
            print(res)
        except Exception as exc:
            self.show_error(exc)
        
#%%
##Metodo 8. Creacion de datos semiestructurados
    def to_xml(self,df):
        def row_xml(row):
            try:
                xml = ['<item>']
                for i, col_name in enumerate(row.index):
                    xml.append('  <{0}>{1}</{0}>'.format(col_name, row.iloc[i]))
                xml.append('</item>')
                
            except Exception as exc:
                self.show_error(exc)
            return '\n'.join(xml)
            
        res = '\n'.join(df.apply(row_xml, axis=1))
        return(res)

#%%
##Metodo 9. Cambio del formato de fecha al estandar de latino america
    def format_publish_date_us_to_latam(self, date_str):
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").strftime('%d/%m/%Y')
        except Exception as exc:
            self.show_error(exc)
        

#%%    
##Metodo 9.1 Normalizacion del formato de fecha
    def normalize_trending_date(self, column_name):
        ##Recorrer elemento por elemento
        try:
            self.data[column_name]=self.data[column_name].apply(lambda date:self.format_date_us_to_latam(str('20'+date)))
        except Exception as exc:
            self.show_error(exc)
        
#%%
##Metodo 9.2 Normalizacion del formato de fecha
    def format_date_us_to_latam(self, date_str):
        try:
            return datetime.strptime(date_str, "%Y.%d.%m").strftime('%d/%m/%Y')
        except Exception as exc:
            self.show_error(exc)
    

#%%   
##Metodo 10. Almecenar datos de DataFrame a un archivo .csv
    def save_data_csv(self, path):                   
        if self.data is not None:
            self.data.to_csv(path)

        
#%%
    # Control de excepciones
    def show_error(self,ex):
        '''
        Captura el tipo de error, su description y localización.
        Parameters
        ----------
        ex : Object
            Exception generada por el sistema.
        Returns
        -------
        None.
        '''
        trace = []
        tb = ex._traceback_
        while tb is not None:
            trace.append({
                          "filename": tb.tb_frame.f_code.co_filename,
                          "name": tb.tb_frame.f_code.co_name,
                          "lineno": tb.tb_lineno
                          })
            
            tb = tb.tb_next
            
        print('{}Something went wrong:'.format(os.linesep))
        print('---type:{}'.format(str(type(ex)._name_)))
        print('---message:{}'.format(str(type(ex))))
        print('---trace:{}'.format(str(trace)))
