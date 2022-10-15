import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df1 = df0.drop(columns=["Critic_Score"])
df2 = df1.drop(columns=["Critic_Count"])
df3 = df2.drop(columns=["User_Score"])
df4 = df3.drop(columns=["User_Count"])
df5 = df4.drop(columns=["Rating"])
df6 = df5.drop(columns=["Developer"])
df7 = df6.dropna(subset=["Name","Platform","Year_of_Release","Genre","Publisher","NA_Sales","EU_Sales","JP_Sales","Other_Sales","Global_Sales"])
df7['Rank'] = df7.index
df7["NA_Sales_Adj"] = df7.apply(lambda xxx__: (xxx__["NA_Sales"] / 579), axis=1)
