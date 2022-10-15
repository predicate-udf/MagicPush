import pickle
import pandas as pd
df0 = pickle.load(open("data_1.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
print(df0.columns)
df1 = df0.drop(columns=["Unnamed: 0","Organization Name URL"])
df2 = df1.drop_duplicates(subset=["Organization Name"])
df3 = df2[df2.apply(lambda row: not (pd.isnull(row['Group Gender'])), axis=1)]
df4 = df3[df3.apply(lambda row: not (pd.isnull(row['Category Groups'])), axis=1)]
df5 = df4[df4.apply(lambda row: not (pd.isnull(row['Headquarters Regions'])), axis=1)]
print(df5.columns)
df6 = df5[df5.apply(lambda row: row['Categories'] == 'Finance', axis=1)]
df7 = pickle.load(open("data_168.pickle",'rb'))
df7.columns = [str(c) for c in df7.columns]
df7["index"] = df7.index
df8 = pd.concat([df7,df6], axis=0)
df9 = pickle.load(open("data_65.pickle",'rb'))
df9.columns = [str(c) for c in df9.columns]
df9["index"] = df9.index
df10 = pd.concat([df8,df9], axis=0)
df11 = df5[df5.apply(lambda row: row['Categories'] == 'Health Care', axis=1)]
df12 = pd.concat([df10,df11], axis=0)
df13 = df5[df5.apply(lambda row: row['Categories'] == 'Internet', axis=1)]
df14 = pd.concat([df12,df13], axis=0)
df15 = df5[df5.apply(lambda row: row['Categories'] == 'Biotechnology', axis=1)]
df16 = pd.concat([df14,df15], axis=0)
df17 = df5[df5.apply(lambda row: row['Categories'] == 'Artificial Intelligence', axis=1)]
df18 = pd.concat([df16,df17], axis=0)
df19 = df5[df5.apply(lambda row: row['Categories'] == 'Information Technology', axis=1)]
df20 = pd.concat([df18,df19], axis=0)
df21 = df5[df5.apply(lambda row: row['Categories'] == 'Education', axis=1)]
df22 = pd.concat([df20,df21], axis=0)
df23 = df5[df5.apply(lambda row: row['Categories'] == 'Advertising', axis=1)]
df24 = pd.concat([df22,df23], axis=0)
df25 = df5[df5.apply(lambda row: row['Categories'] == 'Data', axis=1)]
df26 = pd.concat([df24,df25], axis=0)
df27 = df5[df5.apply(lambda row: row['Categories'] == 'Food', axis=1)]
df28 = pd.concat([df26,df27], axis=0)
df29 = df5[df5.apply(lambda row: row['Categories'] == 'Real Estate', axis=1)]
df30 = pd.concat([df28,df29], axis=0)
df31 = df5[df5.apply(lambda row: row['Categories'] == 'Security', axis=1)]
df32 = pd.concat([df30,df31], axis=0)
df33 = pickle.load(open("data_217.pickle",'rb'))
df33.columns = [str(c) for c in df33.columns]
df33["index"] = df33.index
df34 = pd.concat([df32,df33], axis=0)
df35 = df5[df5.apply(lambda row: row['Categories'] == 'Gaming', axis=1)]
df36 = pd.concat([df34,df35], axis=0)
df37 = df5[df5.apply(lambda row: row['Categories'] == 'Robotics', axis=1)]
df38 = pd.concat([df36,df37], axis=0)
df39 = df5[df5.apply(lambda row: row['Categories'] == 'Fashion', axis=1)]
df40 = pd.concat([df38,df39], axis=0)
df41 = df5[df5.apply(lambda row: row['Categories'] == 'Sports', axis=1)]
df42 = pd.concat([df40,df41], axis=0)
df43 = df5[df5.apply(lambda row: row['Categories'] == 'Tourism', axis=1)]
df44 = pd.concat([df42,df43], axis=0)
df45 = df5[df5.apply(lambda row: row['Categories'] == 'Insurance', axis=1)]
df46 = pd.concat([df44,df45], axis=0)
df47 = df46[df46.apply(lambda row: row['Headquarters Regions'] == 'European Union (EU)', axis=1)]
df48 = df46[df46.apply(lambda row: row['Headquarters Regions'] == 'San Francisco Bay Area, West Coast, Western US', axis=1)]
df49 = pd.concat([df47,df48], axis=0)
df50 = pickle.load(open("data_181.pickle",'rb'))
df50.columns = [str(c) for c in df50.columns]
df50["index"] = df50.index
df51 = pd.concat([df49,df50], axis=0)
df52 = df46[df46.apply(lambda row: row['Headquarters Regions'] == 'Greater New York Area, East Coast, Northeastern US', axis=1)]
df53 = pd.concat([df51,df52], axis=0)
df54 = df46[df46.apply(lambda row: row['Headquarters Regions'] == 'Greater Los Angeles Area, West Coast, Western US', axis=1)]
df55 = pd.concat([df53,df54], axis=0)
pickle.dump(df55, open('./temp/result_original.p', 'wb'))