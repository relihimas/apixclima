#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from datetime import datetime
import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, MetaData, DateTime
import pyodbc


# In[2]:


r = requests.get("http://api.openweathermap.org/data/2.5/weather?id=3390760&Lang=pt_br&units=metric&appid=ef22afe25a0f6f184bdfb6f876e0701c")
weather = r.json()


# In[22]:


data_hora = datetime.today()

table = list()
item = {
    
    "Periodo": data_hora,
    "Cidade": weather['name'],
    "Clima": weather['weather'][0]['main'],
    "Umidade": weather['main']['humidity'],
    "Pressao": weather['main']['pressure'],
    "Temperatura":weather['main']['temp'],
    "TempMax": weather['main']['temp_max'],
    "TempMin": weather['main']['temp_min'],
    "Descricao": weather['weather'][0]['description']

}

table.append(item)
df = pd.DataFrame(table)
#print(df)


# In[19]:


#.format('%Y/%m/%d %H:%M:%S'),
#df.Periodo = df.Periodo.astype('str')
#df.Cidade = df.Cidade.astype('str')
#df.Clima = df.Clima.astype('str')
#df.Descricao = df.Descricao.astype('str')


# In[23]:


df.info()


# In[5]:


SERVER = 'DESKTOP-NPDF171\SQLRACHID'
DATABASE = 'CLIMA'
DRIVER = 'SQL Server Native Client 11.0'
USERNAME = ''
PASSWORD = ''
DATABASE_CONNECTION = f'mssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'


# In[6]:


engine = sqlalchemy.create_engine(DATABASE_CONNECTION)
connection = engine.connect()


# In[27]:


metadata = MetaData()

climarecife = Table('ClimaRecife', metadata,
                        Column('Periodo', DateTime),
                        Column('Cidade', String),
                        Column('Clima', String),
                        Column('Umidade', Integer),
                        Column('Pressao', Integer),
                        Column('Temperatura', Integer),
                        Column('TempMax', Integer),
                        Column('TempMin', Integer),
                        Column('Descricao', String))
                    
inse = climarecife.insert().values([
                        f'{df.Periodo}',
                        f'{df.Cidade}',
                        f'{df.Clima}',
                        f'{df.Umidade}',
                        f'{df.Pressao}',
                        f'{df.Temperatura}',
                        f'{df.TempMax}',
                        f'{df.TempMin}',
                        f'{df.Descricao}'])

    
insira = connection.execute(inse)


# In[ ]:


df.to_csv('def.csv', sep = ",", header= "Periodo, Cidade, Clima, Umidade, Pressao, Temperatura, TempMax, TempMin, Descricao",mode='r')

