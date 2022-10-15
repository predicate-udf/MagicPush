import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df2=pickle.load(open('data_2.pickle','rb'))
df2.drop_duplicates(subset=["postcode"], inplace=True)
df4 = df0.merge(df2, how='left', left_on=["Postcode"],right_on=["postcode"])
df4.drop(columns=["postcode"],inplace=True)
df4.rename(columns={ "Business Category":"business_category" }, inplace=True)



