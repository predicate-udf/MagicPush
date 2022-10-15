import pickle
df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df1 = df0.copy()
df2 = df1[df1.apply(lambda row: row['Element'] == 'TMAX', axis=1)]
df3 = df2.drop(columns=["ID","Element"])
df4 = df3.sort_values(by=["Date"])
df5 = df4.groupby(["Date"]).agg({ "Data_Value":"max","Date":"max" }).rename(columns={ "Data_Value":"Data_Value","Date":"Date" })
pickle.dump(df5, open('./temp/result_original.p', 'wb'))