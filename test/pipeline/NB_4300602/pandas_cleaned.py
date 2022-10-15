import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df16 = df0.copy()
df19 = df16[df16["hotel_id"] == 21]
df22 = df16[df16["hotel_id"] != 21]
df23 = df19.append(df22)

print(df23.shape)
df23['similarity_distance'] = df23.apply(lambda xxx__ : xxx__['distance']*xxx__['distance'], axis=1)
df58 = df23.sort_values(["similarity_distance"])
df59 = df58.reset_index()

