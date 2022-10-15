import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df0.set_index(keys='file_name', inplace=True)
df0.replace(to_replace={ -1:0 }, inplace=True)
df1=pickle.load(open('data_1.pickle','rb'))
df1.set_index(keys='file_name', inplace=True)
df8 = df1.join(df0['young'])
df33 = df8[(df8["partition"] == 0) & (df8["young"] == 0)]

df39 = df8[(df8["partition"] == 0) & (df8["young"] == 1)]
df41 = df33.append(df39)
