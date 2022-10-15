import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df2 = df0.drop(columns=["Events"])
df6 = df2.drop(columns=['Unnamed: 10', 'Sea Level Press. (hPa)', 'Unnamed: 12', 'Unnamed: 13',
       'Visibility (km)', 'Unnamed: 15', 'Unnamed: 16', 'Wind (km/h)',
       'Unnamed: 18', 'Unnamed: 19', 'Precip. (mm)'])
df10 = df6.drop(columns=['Dew Point (°C)', 'Unnamed: 6', 'Unnamed: 7', 'Humidity (%)'])
df12 = df10.drop(columns=["2011"])
df12.rename(columns={ "Unnamed: 0":"date","Temp. (°C)":"highT","Unnamed: 3":"avgT","Unnamed: 4":"lowT","Unnamed: 9":"avgH" }, inplace=True)
df13 = df12[~df12['date'].isnull()]
df17 = df13.set_index(keys='date')
df18 = df17.applymap(int)
print(df18['highT'])
df18['highT'] = df18.apply(lambda xxx__: (xxx__['highT']*(9/5)+32), axis=1)
df18['avgT'] = df18.apply(lambda xxx__: (xxx__['avgT']*(9/5)+32), axis=1)
df18['lowT'] = df18.apply(lambda xxx__: (xxx__['lowT']*(9/5)+32), axis=1)


