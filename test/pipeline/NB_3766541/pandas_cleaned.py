import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df1 = df0.dropna(subset=["Strain","Type","Rating","Effects","Flavor","Description"])
df2 = df1['Effects'].str.get_dummies(sep=',')
df3 = df1['Type'].str.get_dummies(sep=',')
df9 = df2.drop("None", axis=1)
df10 = df1.join(df9, how='inner')
df13 = df10['Flavor'].str.get_dummies(sep=',')
df14 = df13.drop("None", axis=1)
df15 = df10.join(df14, how='inner')
df16 = df15.join(df3, how='inner')
df17 = df16.drop(columns=["Effects","Flavor","Type","Strain","Description"])
