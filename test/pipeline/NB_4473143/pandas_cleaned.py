import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df0.rename(columns={ "Life expectancy at birth (years)  Both sexes":"LE_both" }, inplace=True)
df0.rename(columns={ "Life expectancy at birth (years)  Male":"LE_male" }, inplace=True)
df0.rename(columns={ "Life expectancy at birth (years)  Female":"LE_female" }, inplace=True)
df0.rename(columns={ "GDP per Capita":"GDP" }, inplace=True)
df0.rename(columns={ "Surface area (sq. km)":"Health_expenditure" }, inplace=True)
df0.rename(columns={ "Population, total":"RnD" }, inplace=True)
df16 = df0[(df0["Country"] == "South Sudan") & (df0["Year"] <= 2010)]
df0.reset_index(drop=True, inplace=True)
df0["Income Level"] = df0.apply(lambda xxx__: "Unknown" if xxx__["Income Level"] == np.nan else xxx__["Income Level"], axis=1)
df0.dropna(subset=["Income Level"], inplace=True)
df0["Income Level"] = df0.apply(lambda xxx__: np.nan if xxx__["Income Level"] == "Unknown" else xxx__["Income Level"], axis=1)
df45 = df0.copy()
df48 = df45[df45["Year"] == 2015]
df48.drop(columns=['Country', 'Year', 'LE_male', 'LE_female'],inplace=True)
df48["Income Level"] = df48.apply(lambda xxx__: {'H':0, 'L':1, 'LM':2, 'UM':3}[xxx__["Income Level"]] if xxx__["Income Level"] in ['H', 'L', 'LM', 'UM'] else xxx__["Income Level"], axis=1)
df48["Income Level"] = df48["Income Level"].astype("float")
df48.fillna(0, inplace=True)
print(df48)