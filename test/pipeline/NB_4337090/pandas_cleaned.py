import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df1 = df0.copy()
df4 = df1[df1["Element"] == "TMAX"]
df5 = df4.drop(columns=["ID","Element"])
df6 = df5.sort_values(["Date"])
df9 = df6.groupby(by=["Date"])["Data_Value","Date"].agg(max)

