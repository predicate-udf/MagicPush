import pickle
df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df1=df0.rename(columns={ "0":"index" })
df2 = df1.drop(columns=["4","9"])
df3 = df2.drop(columns=["5","8"])
df4 = pickle.load(open("data_28.pickle",'rb'))
df4.columns = [str(c) for c in df4.columns]
df4["index"] = df4.index
df5=df4.rename(columns={ "0":"index" })
df6 = df5.drop(columns=["6"])
df7=df6.rename(columns={ "1":"Id","2":"College","3":"Course","4":"6","5":"7" })
df8=df3.rename(columns={ "1":"Id","2":"College","3":"Course","6:":"6","7":"7" })
df9 = df8.merge(df7, how='left', left_on = ["Id"], right_on = ["Id"])
df10 = df9
print(df10)
df10["College_x"] = df10["College_x"].astype(str)
df10["College_x"] = df10.apply(lambda xxx__: xxx__['College_x'].replace('\n','').replace('\r',''), axis=1)
df11 = df10
df11["College_y"] = df11["College_y"].astype(str)
df11["College_y"] = df11.apply(lambda xxx__: xxx__['College_y'].replace('\n','').replace('\r',''), axis=1)
pickle.dump(df11, open('./temp/result_original.p', 'wb'))