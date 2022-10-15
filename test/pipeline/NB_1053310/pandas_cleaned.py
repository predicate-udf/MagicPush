import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df1=pickle.load(open('data_1.pickle','rb'))
df5 = df0[df0["CULVERT_COND_062"] == "N"]
df12 = df5
df15 = df12[df12["YEAR_BUILT_027"] > 1900]
df17 = df15[df15["STRUCTURE_LEN_MT_049"] <= 400]
df20 = df1[df1["elevation"] >= 0]
df20.drop(columns=["Unnamed: 0"],inplace=True)
df21 = df17.merge(df20, left_on=["LAT_016","LONG_017"],right_on=["LAT_016","LONG_017"])
df22 = df21.dropna(subset=["STRUCTURE_KIND_043A"])
df22["SUPERSTRUCTURE_COND_059"] = df22["SUPERSTRUCTURE_COND_059"].astype(float)
sr45 = df22.groupby(by=["STRUCTURE_KIND_043A"])["SUPERSTRUCTURE_COND_059"].mean().reset_index()

