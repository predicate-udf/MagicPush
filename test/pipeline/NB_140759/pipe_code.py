import pickle
df0 = pickle.load(open("data_0.pickle",'rb'))
print(df0.columns)
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df1=df0.rename(columns={ "file_name":"index" })
df2 = df1
df2["hair_color"] = df2.apply(lambda xxx__: { -1:0 }[xxx___["hair_color"]] if xxx___["hair_color"] in [-1] else  xxx___["hair_color"], axis=1)
df3 = df2
df3["eyeglasses"] = df3.apply(lambda xxx__: { -1:0 }[xxx___["eyeglasses"]] if xxx___["eyeglasses"] in [-1] else  xxx___["eyeglasses"], axis=1)
df4 = df3
df4["smiling"] = df4.apply(lambda xxx__: { -1:0 }[xxx___["smiling"]] if xxx___["smiling"] in [-1] else  xxx___["smiling"], axis=1)
df5 = df4
df5["young"] = df5.apply(lambda xxx__: { -1:0 }[xxx___["young"]] if xxx___["young"] in [-1] else  xxx___["young"], axis=1)
df6 = df5
df6["human"] = df6.apply(lambda xxx__: { -1:0 }[xxx___["human"]] if xxx___["human"] in [-1] else  xxx___["human"], axis=1)
df7 = pickle.load(open("data_1.pickle",'rb'))
print(df7.columns)
df7.columns = [str(c) for c in df7.columns]
df7["index"] = df7.index
df8=df7.rename(columns={ "file_name":"index" })
df9 = df8[(df8['partition'] == 0) & (df8['young'] == 0)]
df10 = df8[(df8['partition'] == 0) & (df8['young'] == 1)]