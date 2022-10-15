import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df1=pickle.load(open('data_1.pickle','rb'))
df2=pickle.load(open('data_2.pickle','rb'))
df3=pickle.load(open('data_3.pickle','rb'))
df4=pickle.load(open('data_4.pickle','rb'))
df0["Date"] = pd.to_datetime(df0["Date"])
df1["Date"] = pd.to_datetime(df1["Date"])
df2["Date"] = pd.to_datetime(df2["Date"])
df3["Date"] = pd.to_datetime(df3["Date"])
df4["Date"] = pd.to_datetime(df4["Date"])
df5 = pd.concat([df0, df1, df2, df3, df4], axis=0)
print(df5)
