import pickle
df0 = pickle.load(open("data_9.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df1 = df0.drop(columns=[])
df2=df1.rename(columns={ "Cumulative cases of COVID-19 in the U.S. from January 22 to May 8, 2020, by day":"Date","Unnamed: 1":"Cumulative_Cases" })
df3 = df2.sort_values(by=["index"])
df4 = df3.dropna(subset=["Cumulative_Cases"])
df5 = df4
df5["Cumulative_Cases"] = df5.apply(lambda xxx__: str(xxx__["Cumulative_Cases"]).replace(",",''), axis=1)
df6 = df5
df6['Cumulative_Cases'] = df6['Cumulative_Cases'].astype('str')
pickle.dump(df6, open('./temp/result_original.p', 'wb'))