import pickle
import pandas as pd
df0 = pickle.load(open("data_0.pickle",'rb'))
print(df0.dtypes)
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
print(df0.dtypes)
size__before = df0.shape[0]
print(df0['Temp. (°C)'])
print(df0['Unnamed: 3'])
df0['Temp. (°C)'] = df0['Temp. (°C)'].astype("float")
df0['Unnamed: 3'] = df0['Temp. (°C)'].astype("float")
df0['Unnamed: 4'] = df0['Temp. (°C)'].astype("float")
print(df0['Temp. (°C)'])
print(df0['Unnamed: 3'])
df0 = df0[df0.apply(lambda row: (not pd.isnull(row['Unnamed: 0'])) and ( (row['Temp. (°C)']*(9/5)+32)) == 80.6 and ( (row['Unnamed: 3']*(9/5)+32)) == 73.4 and ( (row['Unnamed: 4']*(9/5)+32)) == 64.4 and (row['Unnamed: 9'] == 62) and (row['Unnamed: 0'] == '9/16/15 12:00 AM') and (row['Unnamed: 0'] == 1832), axis=1)]
size__after = df0.shape[0]
print("Input data_0.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
df0_0 = df0[df0.apply(lambda row: (not pd.isnull(row['Unnamed: 0'])) and ( (row['Temp. (°C)']*(9/5)+32)) == 80.6 and ( (row['Unnamed: 3']*(9/5)+32)) == 73.4 and ( (row['Unnamed: 4']*(9/5)+32)) == 64.4 and (row['Unnamed: 9'] == 62) and (row['Unnamed: 0'] == '9/16/15 12:00 AM') and (row['Unnamed: 0'] == 1832), axis=1)]
df1 = df0_0.drop(columns=["Events"])
df1_0 = df1[df1.apply(lambda row: (not pd.isnull(row['Unnamed: 0'])) and ( (row['Temp. (°C)']*(9/5)+32)) == 80.6 and ( (row['Unnamed: 3']*(9/5)+32)) == 73.4 and ( (row['Unnamed: 4']*(9/5)+32)) == 64.4 and (row['Unnamed: 9'] == 62) and (row['Unnamed: 0'] == '9/16/15 12:00 AM') and (row['Unnamed: 0'] == 1832), axis=1)]
df2 = df1_0.drop(columns=["Unnamed: 10","Sea Level Press. (hPa)","Unnamed: 12","Unnamed: 13","Visibility (km)","Unnamed: 15","Unnamed: 16","Wind (km/h)","Unnamed: 18","Unnamed: 19","Precip. (mm)"])
df2_0 = df2[df2.apply(lambda row: (not pd.isnull(row['Unnamed: 0'])) and ( (row['Temp. (°C)']*(9/5)+32)) == 80.6 and ( (row['Unnamed: 3']*(9/5)+32)) == 73.4 and ( (row['Unnamed: 4']*(9/5)+32)) == 64.4 and (row['Unnamed: 9'] == 62) and (row['Unnamed: 0'] == '9/16/15 12:00 AM') and (row['Unnamed: 0'] == 1832), axis=1)]
df3 = df2_0.drop(columns=["Dew Point (°C)","Unnamed: 6","Unnamed: 7","Humidity (%)"])
df3_0 = df3[df3.apply(lambda row: (not pd.isnull(row['Unnamed: 0'])) and ( (row['Temp. (°C)']*(9/5)+32)) == 80.6 and ( (row['Unnamed: 3']*(9/5)+32)) == 73.4 and ( (row['Unnamed: 4']*(9/5)+32)) == 64.4 and (row['Unnamed: 9'] == 62) and (row['Unnamed: 0'] == '9/16/15 12:00 AM') and (row['Unnamed: 0'] == 1832), axis=1)]
df4 = df3_0.drop(columns=["2011"])
df4_0 = df4[df4.apply(lambda row: (not pd.isnull(row['Unnamed: 0'])) and ( (row['Temp. (°C)']*(9/5)+32)) == 80.6 and ( (row['Unnamed: 3']*(9/5)+32)) == 73.4 and ( (row['Unnamed: 4']*(9/5)+32)) == 64.4 and (row['Unnamed: 9'] == 62) and (row['Unnamed: 0'] == '9/16/15 12:00 AM') and (row['Unnamed: 0'] == 1832), axis=1)]
df5=df4_0.rename(columns={ "Unnamed: 0":"date","Temp. (°C)":"highT","Unnamed: 3":"avgT","Unnamed: 4":"lowT","Unnamed: 9":"avgH" })
df5_0 = df5[df5.apply(lambda row: (not pd.isnull(row['date'])) and ( (row['highT']*(9/5)+32)) == 80.6 and ( (row['avgT']*(9/5)+32)) == 73.4 and ( (row['lowT']*(9/5)+32)) == 64.4 and (row['avgH'] == 62) and (row['date'] == '9/16/15 12:00 AM') and (row['date'] == 1832), axis=1)]
df6 = df5_0[df5_0.apply(lambda row: not pd.isnull(row['date']), axis=1)]
df6_0 = df6[df6.apply(lambda row: ( (row['highT']*(9/5)+32)) == 80.6 and ( (row['avgT']*(9/5)+32)) == 73.4 and ( (row['lowT']*(9/5)+32)) == 64.4 and (row['avgH'] == 62) and (row['date'] == '9/16/15 12:00 AM') and (row['date'] == 1832), axis=1)]
df6_0['index'] = df6_0['date']
df7=df6_0.drop(columns=['date'])
df7_0 = df7[df7.apply(lambda row: ( (row['highT']*(9/5)+32)) == 80.6 and ( (row['avgT']*(9/5)+32)) == 73.4 and ( (row['lowT']*(9/5)+32)) == 64.4 and (row['avgH'] == 62) and (row['index'] == '9/16/15 12:00 AM') and (row['index'] == 1832), axis=1)]
df8 = df7_0
df8['highT'] = df8['highT'].astype('int')
df8_0 = df8[df8.apply(lambda row: ( (row['highT']*(9/5)+32)) == 80.6 and ( (row['avgT']*(9/5)+32)) == 73.4 and ( (row['lowT']*(9/5)+32)) == 64.4 and (row['avgH'] == 62) and (row['index'] == '9/16/15 12:00 AM') and (row['index'] == 1832), axis=1)]
df9 = df8_0
df9['avgT'] = df9['avgT'].astype('int')
df9_0 = df9[df9.apply(lambda row: ( (row['highT']*(9/5)+32)) == 80.6 and ( (row['avgT']*(9/5)+32)) == 73.4 and ( (row['lowT']*(9/5)+32)) == 64.4 and (row['avgH'] == 62) and (row['index'] == '9/16/15 12:00 AM') and (row['index'] == 1832), axis=1)]
df10 = df9_0
df10['lowT'] = df10['lowT'].astype('int')
df10_0 = df10[df10.apply(lambda row: ( (row['highT']*(9/5)+32)) == 80.6 and ( (row['avgT']*(9/5)+32)) == 73.4 and ( (row['lowT']*(9/5)+32)) == 64.4 and (row['avgH'] == 62) and (row['index'] == '9/16/15 12:00 AM') and (row['index'] == 1832), axis=1)]
df11 = df10_0
df11['avgH'] = df11['avgH'].astype('int')
df11_0 = df11[df11.apply(lambda row: ( (row['highT']*(9/5)+32)) == 80.6 and ( (row['avgT']*(9/5)+32)) == 73.4 and ( (row['lowT']*(9/5)+32)) == 64.4 and (row['avgH'] == 62) and (row['index'] == '9/16/15 12:00 AM') and (row['index'] == 1832), axis=1)]
df12 = df11_0
df12["highT"] = df12.apply(lambda xxx__: (xxx__['highT']*(9/5)+32), axis=1)
df12_0 = df12[df12.apply(lambda row: (row['highT'] == 80.6) and ( (row['avgT']*(9/5)+32)) == 73.4 and ( (row['lowT']*(9/5)+32)) == 64.4 and (row['avgH'] == 62) and (row['index'] == '9/16/15 12:00 AM') and (row['index'] == 1832), axis=1)]
df13 = df12_0
df13["avgT"] = df13.apply(lambda xxx__: (xxx__['avgT']*(9/5)+32), axis=1)
df13_0 = df13[df13.apply(lambda row: (row['highT'] == 80.6) and (row['avgT'] == 73.4) and ( (row['lowT']*(9/5)+32)) == 64.4 and (row['avgH'] == 62) and (row['index'] == '9/16/15 12:00 AM') and (row['index'] == 1832), axis=1)]
df14 = df13_0
df14["lowT"] = df14.apply(lambda xxx__: (xxx__['lowT']*(9/5)+32), axis=1)
df14_0 = df14[df14.apply(lambda row: (row['highT'] == 80.6) and (row['avgT'] == 73.4) and (row['lowT'] == 64.4) and (row['avgH'] == 62) and (row['index'] == '9/16/15 12:00 AM') and (row['index'] == 1832), axis=1)]
pickle.dump(df14, open('temp//result_after_pushdown.p', 'wb'))