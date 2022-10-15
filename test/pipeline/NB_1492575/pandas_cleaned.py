import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df0.columns = [str(x) for x in df0.columns]
df0.drop(columns=['26', '27'],inplace=True)
df0.rename(columns={ "0":"id","1":"cycle","2":"setting1","3":"setting2","4":"setting3","5":"s1","6":"s2","7":"s3","8":"s4","9":"s5","10":"s6","11":"s7","12":"s8","13":"s9","14":"s10","15":"s11","16":"s12","17":"s13","18":"s14","19":"s15","20":"s16","21":"s17","22":"s18","23":"s19","24":"s20","25":"s21" }, inplace=True)
df5 = df0.sort_values(["id","cycle"])
df27=pickle.load(open('data_27.pickle','rb'))
df27.columns = [str(x) for x in df27.columns]
df27.drop(columns=["26",'27'],inplace=True)
df27.rename(columns={ "0":"id","1":"cycle","2":"setting1","3":"setting2","4":"setting3","5":"s1","6":"s2","7":"s3","8":"s4","9":"s5","10":"s6","11":"s7","12":"s8","13":"s9","14":"s10","15":"s11","16":"s12","17":"s13","18":"s14","19":"s15","20":"s16","21":"s17","22":"s18","23":"s19","24":"s20","25":"s21" }, inplace=True)
df32=pickle.load(open('data_32.pickle','rb'))
df32.columns = [str(x) for x in df32.columns]
df32.drop(columns=["1"],inplace=True)
df32.rename(columns={ "0":"more" }, inplace=True)
df42 = df5.groupby(by=["id"])["cycle"].transform(max).reset_index().rename(columns={'index':'id'})
df43 = df5.merge(df42, on=['id'])
df43['RUL'] = df43.apply(lambda xxx__: xxx__['cycle_y']-xxx__['cycle_x'], axis=1)
df44 = df43.drop(columns=['cycle_y'])

print(df42)
print(df5)

print(df43)