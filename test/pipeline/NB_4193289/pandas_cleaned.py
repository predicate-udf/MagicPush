import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df1=pickle.load(open('data_1.pickle','rb'))
df2 = df0.drop(columns=["Id"])
df3 = df1.drop(columns=["Id"])
df10 = df2.drop(columns=["Alley","FireplaceQu","PoolQC","Fence","MiscFeature"])
df11 = df3.drop(columns=["Alley","FireplaceQu","PoolQC","Fence","MiscFeature"])
df18 = df10.append(df11)
