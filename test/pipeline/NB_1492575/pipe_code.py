import pickle
df0 = pickle.load(open("NB_1502454/data_187.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df1=df0.rename(columns={ "@OBS_VALUE":"US.LE_IX","@TIME_PERIOD":"date" })
df2=df0.rename(columns={ "@OBS_VALUE":"US.PCPI_IX","@TIME_PERIOD":"date" })
df3 = df1.merge(df2, left_on = ["date"], right_on = ["date"])
df4 = pickle.load(open("NB_1502454/data_403.pickle",'rb'))
df4.columns = [str(c) for c in df4.columns]
df4["index"] = df4.index
df5=df4.rename(columns={ "@OBS_VALUE":"US.ENDE_XDC_USD_RATE","@TIME_PERIOD":"date" })
df6 = df3.merge(df5, left_on = ["date"], right_on = ["date"])
df7 = pickle.load(open("NB_1502454/data_514.pickle",'rb'))
df7.columns = [str(c) for c in df7.columns]
df7["index"] = df7.index
df8=df7.rename(columns={ "@OBS_VALUE":"KR.LE_IX","@TIME_PERIOD":"date" })
df9 = df6.merge(df8, left_on = ["date"], right_on = ["date"])
df10 = pickle.load(open("NB_1502454/data_626.pickle",'rb'))
df10.columns = [str(c) for c in df10.columns]
df10["index"] = df10.index
df11=df10.rename(columns={ "@OBS_VALUE":"KR.PCPI_IX","@TIME_PERIOD":"date" })
df12 = df9.merge(df11, left_on = ["date"], right_on = ["date"])
df13 = pickle.load(open("NB_1502454/data_405.pickle",'rb'))
df13.columns = [str(c) for c in df13.columns]
df13["index"] = df13.index
df14=df13.rename(columns={ "@OBS_VALUE":"KR.ENDE_XDC_USD_RATE","@TIME_PERIOD":"date" })
df15 = df12.merge(df14, left_on = ["date"], right_on = ["date"])
df16 = pickle.load(open("NB_1502454/data_849.pickle",'rb'))
df16.columns = [str(c) for c in df16.columns]
df16["index"] = df16.index
df17=df16.rename(columns={ "@OBS_VALUE":"JP.LE_IX","@TIME_PERIOD":"date" })
df18 = df15.merge(df17, left_on = ["date"], right_on = ["date"])
df19=df7.rename(columns={ "@OBS_VALUE":"JP.PCPI_IX","@TIME_PERIOD":"date" })
df20 = df18.merge(df19, left_on = ["date"], right_on = ["date"])
df21 = pickle.load(open("NB_1502454/data_1072.pickle",'rb'))
df21.columns = [str(c) for c in df21.columns]
df21["index"] = df21.index
df22=df21.rename(columns={ "@OBS_VALUE":"JP.ENDE_XDC_USD_RATE","@TIME_PERIOD":"date" })
df23 = df20.merge(df22, left_on = ["date"], right_on = ["date"])
df24 = pickle.load(open("NB_1502454/data_1184.pickle",'rb'))
df24.columns = [str(c) for c in df24.columns]
df24["index"] = df24.index
df25=df24.rename(columns={ "@OBS_VALUE":"RU.LE_IX","@TIME_PERIOD":"date" })
df26 = df23.merge(df25, left_on = ["date"], right_on = ["date"])
df27=df21.rename(columns={ "@OBS_VALUE":"RU.PCPI_IX","@TIME_PERIOD":"date" })
df28 = df26.merge(df27, left_on = ["date"], right_on = ["date"])
df29 = pickle.load(open("NB_1502454/data_1407.pickle",'rb'))
df29.columns = [str(c) for c in df29.columns]
df29["index"] = df29.index
df30=df29.rename(columns={ "@OBS_VALUE":"RU.ENDE_XDC_USD_RATE","@TIME_PERIOD":"date" })
df31 = df28.merge(df30, left_on = ["date"], right_on = ["date"])
df32 = df31.drop(columns=[])
df33 = pickle.load(open("NB_1502454/data_1416.pickle",'rb'))
df33.columns = [str(c) for c in df33.columns]
df33["index"] = df33.index
df34 = df33[df33.apply(lambda row: not pd.isnull(row['marketcap(USD)']), axis=1)]
df35 = df34.dropna(subset=["date","marketcap(USD)"])
df36 = df35.merge(df32, how='left', left_on = ["date"], right_on = ["date"])
df37 = df36
df37['date'] = pd.to_datetime(df37['date'])
df38 = df37[df37.apply(lambda row: row['date'] < pd.to_datetime(''2016-06-01 00:00:00''), axis=1)]
df39 = df38[df38.apply(lambda row: row['date'] >= pd.to_datetime(''2016-01-01 00:00:00'') and row['date'] < pd.to_datetime(''2016-06-01 00:00:00''), axis=1)]
pickle.dump(df39, open('./temp/result_original.p', 'wb'))