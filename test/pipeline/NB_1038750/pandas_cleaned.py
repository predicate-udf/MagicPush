import pandas as pd
import pickle
df0=pickle.load(open('data_0.pickle','rb'))
df6 = df0.groupby(by=["Data","País/Região"], as_index=False).agg({
    'Latitude': 'mean',
    'Longitude': 'mean',
    'PIB 2019': 'unique',
    'População': 'unique',
    'População Urbana': 'unique',
    'Área (Km/2)': 'unique',
    'Densidade (Km/2)': 'unique',
    'Taxa de Fertilidade': 'mean',
    'Média de Idade': 'mean',
    'Taxa de Fumantes': 'mean',
    'Taxa de Mortalidade por Doenças Pulmonares': 'mean',
    'Total de Leitos Hospitalares': 'min',
    'Temperatura Média (Janeiro - Março)': 'mean',
    'Taxa de Umidade (Janeiro - Março)': 'mean',
    'Casos Confirmados': 'sum'
})
df35 = df6.sort_values(["Data","País/Região"])
df36 = df35.drop_duplicates(subset=["País/Região"])
df37 = df36[["País/Região","Latitude","Longitude","PIB 2019","População","População Urbana","Área (Km/2)","Densidade (Km/2)","Taxa de Fertilidade","Média de Idade","Taxa de Fumantes","Taxa de Mortalidade por Doenças Pulmonares","Total de Leitos Hospitalares","Temperatura Média (Janeiro - Março)","Taxa de Umidade (Janeiro - Março)"]]
df41 = df35.pivot(index='País/Região',columns='Data',values='Casos Confirmados')
df42 = df41.reset_index()

df52 = df37.merge(df42, how='left', left_on=["País/Região"],right_on=["País/Região"])
df53 = df52[df52['País/Região']=='Afghanistan']
df54 = df53[['País/Região', 'Latitude', 'Longitude','População Urbana','4/15/20']]
