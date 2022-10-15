import pandas as pd
import pickle
import numpy as np
df1=pickle.load(open('data_1.pickle','rb'))
df2=pickle.load(open('data_2.pickle','rb'))
df3=pickle.load(open('data_3.pickle','rb'))
df4 = df2.append(df3)
df5=pickle.load(open('data_5.pickle','rb'))
df6 = df4.append(df5)
df7=pickle.load(open('data_7.pickle','rb'))
df8 = df6.append(df7)
df10 = df1.append(df8)
