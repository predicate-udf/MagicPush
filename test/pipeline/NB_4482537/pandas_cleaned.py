import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df0.drop_duplicates(subset=["timestamp","property","language"], inplace=True)
df0.sort_values(["property","timestamp"], inplace=True)
df5 = df0[~df0["language"].isnull()]
df5['rating'] = 1
df8 = df5.pivot(index='property',columns='language',values='rating')
print(list(df8.columns))
print(df8)
df8.fillna(0, inplace=True)

