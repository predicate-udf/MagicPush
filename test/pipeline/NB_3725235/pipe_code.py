import pickle
import pandas as pd
df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
size__before = df0.shape[0]
df0 = df0[df0.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
size__after = df0.shape[0]
print("Input data_0.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
df0_0 = df0[df0.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
df1 = pickle.load(open("data_1.pickle",'rb'))
df1.columns = [str(c) for c in df1.columns]
df1["index"] = df1.index
size__before = df1.shape[0]
df1 = df1[df1.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
size__after = df1.shape[0]
print("Input data_1.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
df1_0 = df1[df1.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
df2 = pickle.load(open("data_2.pickle",'rb'))
df2.columns = [str(c) for c in df2.columns]
df2["index"] = df2.index
size__before = df2.shape[0]
df2 = df2[df2.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
size__after = df2.shape[0]
print("Input data_2.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
df2_0 = df2[df2.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
df3 = pickle.load(open("data_3.pickle",'rb'))
df3.columns = [str(c) for c in df3.columns]
df3["index"] = df3.index
size__before = df3.shape[0]
df3 = df3[df3.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
size__after = df3.shape[0]
print("Input data_3.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
df3_0 = df3[df3.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
df4 = pickle.load(open("data_4.pickle",'rb'))
df4.columns = [str(c) for c in df4.columns]
df4["index"] = df4.index
size__before = df4.shape[0]
df4 = df4[df4.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
size__after = df4.shape[0]
print("Input data_4.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
df4_0 = df4[df4.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
df5 = df0_0
df5['Date'] = pd.to_datetime(df5['Date'])
df5_0 = df5[df5.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
df6 = df1_0
df6['Date'] = pd.to_datetime(df6['Date'])
df6_0 = df6[df6.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
df7 = df2_0
df7['Date'] = pd.to_datetime(df7['Date'])
df7_0 = df7[df7.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
df8 = df3_0
df8['Date'] = pd.to_datetime(df8['Date'])
df8_0 = df8[df8.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
df9 = df4_0
df9['Date'] = pd.to_datetime(df9['Date'])
df9_0 = df9[df9.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
df10 = pd.concat([df5_0,df6_0,df7_0,df8_0,df9_0], axis=0)
df10_0 = df10[df10.apply(lambda row: (row['Date'] == pd.to_datetime('2017-01-09 00:00:00')) and (row['Open'] == 15.604) and (row['High'] == 16.034401000000003) and (row['Low'] == 15.4964) and (row['Close'] == 15.8192) and (row['Adj Close'] == 15.8192) and (row['Volume'] == 293662) and (row['index'] == 500) and (row['index'] == 500), axis=1)]
pickle.dump(df10, open('temp//result_after_pushdown.p', 'wb'))