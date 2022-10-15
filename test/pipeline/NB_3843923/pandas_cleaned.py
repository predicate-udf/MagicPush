import pandas as pd
import pickle
df22=pickle.load(open('data_22.pickle','rb'))
df23 = pd.melt(df22, id_vars=["country","year"],value_vars=["m4554","mu","m2534","m1524","m3544","m014","m5564","m65","f014"],var_name='sex_and_age',value_name='cases')
df26 = df23['sex_and_age'].str.extract("(\D)(\d+)(\d{2})",expand=False)
df26.rename(columns={ 0:"sex",1:"age_lower",2:"age_upper" }, inplace=True)
df26["age"] = df26.apply(lambda xxx__: str(xxx__['age_lower'])+"-"+str(xxx__['age_upper']), axis=1)
print(df23.head())
print(df26.head())
df30 = pd.concat([df23,df26], axis=1)
print(df30.head())
df31 = df30.drop(columns=["sex_and_age","age_lower","age_upper"])
df32 = df31.dropna(subset=["country","year","cases","sex","age"])
df33 = df32.sort_values(["country","year","sex","age"])

print(df33['age'].dtype)