import pandas as pd
import pickle
import numpy as np
df9=pickle.load(open('data_9.pickle','rb'))
print(df9.columns)
df10 = df9.drop(columns=[])
df12 = df10.rename(columns={ "Cumulative cases of COVID-19 in the U.S. from January 22 to May 8, 2020, by day":"Date","Unnamed: 1":"Cumulative_Cases" })
df13 = df12.reset_index()
df13 = df13.dropna(subset=["Cumulative_Cases"])
df13['Cumulative_Cases'] = df13.apply(lambda xxx__: str(xxx__["Cumulative_Cases"]).replace(",",''), axis=1)
df13["Cumulative_Cases"] = df13["Cumulative_Cases"].astype("int")
