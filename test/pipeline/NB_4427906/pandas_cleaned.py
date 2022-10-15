import pandas as pd
import pickle
df1=pickle.load(open('data_1.pickle','rb'))
print(df1)
df1.fillna(0, inplace=True)
df21 = df1[df1["hours_worked_per_week"] > 0]
df36 = df21.copy()
print(df36)
df36["female"] = df36.apply(lambda xxx__: ({ "Female":1,"Male":0 }[xxx__["gender"]] if xxx__["gender"] in ["Female","Male"] else xxx__["gender"]), axis=1)
print(df36.columns)
df36["person_hard_worker"]=df36.apply(lambda xxx__: 1 if xxx__['hours_worked_per_week'] > 38.8 else 0)

print(df36)