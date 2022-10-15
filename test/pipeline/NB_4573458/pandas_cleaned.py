import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df0.drop(columns=["1"],inplace=True)
df0.rename(columns={ "usual":"parents","proper":"has_nurs","complete":"form","convenient":"children","convenient.1":"housing","nonprob":"finance","recommended":"social","recommend":"health" }, inplace=True)
sr3 = df0.drop(columns=['parents', 'has_nurs', 'form', 'children', 'housing', 'finance', 'social'])
df0.drop(columns=["health"],inplace=True)
df4 = pd.get_dummies(df0)
#df21 = pd.concat([sr3,df4], axis=1)
df21 = sr3.merge(df4, left_index=True, right_index=True, how='inner')
df21.replace(to_replace={ "not_recom":0 }, inplace=True)
df21.replace(to_replace={ "priority":1 }, inplace=True)
df21.replace(to_replace={ "spec_prior":2 }, inplace=True)
df21.replace(to_replace={ "very_recom":3 }, inplace=True)
df21.replace(to_replace={ "recommend":4 }, inplace=True)
df21.dropna(inplace=True)

