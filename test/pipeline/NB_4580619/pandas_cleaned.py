import pandas as pd
import pickle
import numpy as np
df21=pickle.load(open('data_21.pickle','rb'))
df22 = pd.melt(df21, id_vars=["country","year"],value_vars=["m5564","m2534","mu","m65","m4554","m1524","m3544","f014","m014"],var_name='sex_and_age',value_name='cases')
df25 = df22["sex_and_age"].str.extract("(\D)(\d+)(\d{2})", expand=False)
df25.rename(columns={ 0:"sex",1:"age_lower",2:"age_upper" }, inplace=True)
df25["age"] = df25["age_lower"] + "-" + df25["age_upper"]
df30 = pd.concat([df22,df25], axis=1)
df31 = df30.drop(columns=["sex_and_age","age_lower","age_upper"])
df32 = df31.dropna(subset=["country","year","cases","sex","age"])
df33 = df32.sort_values(["country","year","sex","age"])
