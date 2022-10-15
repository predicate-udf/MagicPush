import pickle
import pandas as pd
# df0 = pickle.load(open("data_0.pickle",'rb'))
# df0.columns = [str(c) for c in df0.columns]
# df0["index"] = df0.index
# df1 = df0.drop_duplicates(subset=["timestamp","property","language"])
# df2 = df1.sort_values(by=["property","timestamp"])
# df3 = df2[df2.apply(lambda row: not (pd.isnull(row['language'])), axis=1)]
# df4 = df3
# df4["rating"] = df4.apply(lambda xxx__: 1, axis=1)
# df5 = df4.pivot(index="property", columns="language", values="rating").reset_index()
# print(list(df5.columns))
# print(df5)
# pickle.dump(df5, open('./temp/result_original.p', 'wb'))
# df0 = pickle.load(open("data_0.pickle",'rb'))
# df0.columns = [str(c) for c in df0.columns]
# df0["index"] = df0.index
# df1 = df0.drop_duplicates(subset=["timestamp","property","language"])
# df2 = df1.sort_values(by=["property","timestamp"])
# df3 = df2[df2.apply(lambda row: not (pd.isnull(row['language'])), axis=1)]
# df4 = df3
# df4["rating"] = df4.apply(lambda xxx__: 1, axis=1)
# df5 = df4.pivot(index="property", columns="language", values="rating").reset_index()
# final_output = df5[df5.apply(lambda row: (row['property'] == 'P447') and (pd.isnull(row['En'])) and (pd.isnull(row['aa'])) and (pd.isnull(row['ace'])) and (pd.isnull(row['aeb-arab'])) and (pd.isnull(row['aeb-latn'])), axis=1)]
# print(final_output)
# pickle.dump(final_output, open('temp//result_pred_on_output.p', 'wb'))


df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
size__before = df0.shape[0]
df0 = df0[df0.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P447') and (row['language'] == 'En') or (row['property'] == 'P447')  and (row['language'] == 'aa') or (row['property'] == 'P447') and (row['language'] == 'ace') or (row['property'] == 'P447')  and (row['language'] == 'aeb-arab') or (row['property'] == 'P447') and (row['language'] == 'aeb-latn'), axis=1)]
size__after = df0.shape[0]
print("Input data_0.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
df0_0 = df0[df0.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P447')  and (row['language'] == 'En') or (row['property'] == 'P447')  and (row['language'] == 'aa') or (row['property'] == 'P447') and (row['language'] == 'ace') or (row['property'] == 'P447') and (row['language'] == 'aeb-arab') or (row['property'] == 'P447') and (row['language'] == 'aeb-latn'), axis=1)]
df1 = df0_0.drop_duplicates(subset=["timestamp","property","language"])
df1_0 = df1[df1.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P447')  and (row['language'] == 'En') or (row['property'] == 'P447')  and (row['language'] == 'aa') or (row['property'] == 'P447')  and (row['language'] == 'ace') or (row['property'] == 'P447') and (row['language'] == 'aeb-arab') or (row['property'] == 'P447') and (row['language'] == 'aeb-latn'), axis=1)]
df2 = df1_0.sort_values(by=["property","timestamp"])
df2_0 = df2[df2.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P447') and (row['language'] == 'En') or (row['property'] == 'P447')  and (row['language'] == 'aa') or (row['property'] == 'P447')  and (row['language'] == 'ace') or (row['property'] == 'P447') and (row['language'] == 'aeb-arab') or (row['property'] == 'P447') and (row['language'] == 'aeb-latn'), axis=1)]
df3 = df2_0[df2_0.apply(lambda row: not (pd.isnull(row['language'])), axis=1)]
df3_0 = df3[df3.apply(lambda row: (row['property'] == 'P447')  and (row['language'] == 'En') or (row['property'] == 'P447')  and (row['language'] == 'aa') or (row['property'] == 'P447')  and (row['language'] == 'ace') or (row['property'] == 'P447')  and (row['language'] == 'aeb-arab') or (row['property'] == 'P447')  and (row['language'] == 'aeb-latn'), axis=1)]
df4 = df3_0
df4["rating"] = df4.apply(lambda xxx__: 1, axis=1)
df4_0 = df4[df4.apply(lambda row: (row['property'] == 'P447') and (row['language'] == 'En') or (row['property'] == 'P447') and (row['language'] == 'aa') or (row['property'] == 'P447') and (row['language'] == 'ace') or (row['property'] == 'P447')and (row['language'] == 'aeb-arab') or (row['property'] == 'P447') and (row['language'] == 'aeb-latn'), axis=1)]
df5 = df4_0.pivot(index="property", columns="language", values="rating").reset_index()

df5_0 = df5[df5.apply(lambda row: (row['property'] == 'P447') and (pd.isnull(row['En'])) and (pd.isnull(row['aa'])) and (pd.isnull(row['ace'])) and (pd.isnull(row['aeb-arab'])) and (pd.isnull(row['aeb-latn'])), axis=1)]

pickle.dump(df5, open('temp//result_after_pushdown.p', 'wb'))


# df0 = pickle.load(open("data_0.pickle",'rb'))
# df0.columns = [str(c) for c in df0.columns]
# df0["index"] = df0.index
# size__before = df0.shape[0]
# df0 = df0[df0.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == En) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aa) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == ace) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-arab) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-latn) or (row['property'] == 'P1616') and (None == 557) and (row['language'] == index), axis=1)]
# size__after = df0.shape[0]
# print("Input data_0.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
# df0_0 = df0[df0.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == En) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aa) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == ace) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-arab) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-latn) or (row['property'] == 'P1616') and (None == 557) and (row['language'] == index), axis=1)]
# df1 = df0_0.drop_duplicates(subset=["timestamp","property","language"])
# df1_0 = df1[df1.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == En) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aa) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == ace) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-arab) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-latn) or (row['property'] == 'P1616') and (None == 557) and (row['language'] == index), axis=1)]
# df2 = df1_0.sort_values(by=["property","timestamp"])
# df2_0 = df2[df2.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == En) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aa) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == ace) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-arab) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-latn) or (row['property'] == 'P1616') and (None == 557) and (row['language'] == index), axis=1)]
# df3 = df2_0[df2_0.apply(lambda row: not (pd.isnull(row['language'])), axis=1)]
# df3_0 = df3[df3.apply(lambda row: (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == En) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aa) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == ace) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-arab) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-latn) or (row['property'] == 'P1616') and (None == 557) and (row['language'] == index), axis=1)]
# df4 = df3_0
# df4["rating"] = df4.apply(lambda xxx__: 1, axis=1)
# df4_0 = df4[df4.apply(lambda row: (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == En) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aa) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == ace) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-arab) or (row['property'] == 'P1616') and (pd.isnull(None)) and (row['language'] == aeb-latn) or (row['property'] == 'P1616') and (None == 557) and (row['language'] == index), axis=1)]
# df5 = df4_0.pivot(index="property", columns="language", values="rating").reset_index()
# df5_0 = df5[df5.apply(lambda row: (row['property'] == 'P1616') and (pd.isnull(row['En'])) and (pd.isnull(row['aa'])) and (pd.isnull(row['ace'])) and (pd.isnull(row['aeb-arab'])) and (pd.isnull(row['aeb-latn'])) and (row['index'] == 557), axis=1)]
# pickle.dump(df5, open('temp//result_after_pushdown.p', 'wb'))

# df0 = pickle.load(open("data_0.pickle",'rb'))
# df0.columns = [str(c) for c in df0.columns]
# df0["index"] = df0.index
# size__before = df0.shape[0]
# df0 = df0[df0.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P4188') and ( 0 if pd.isnull(row["En"]) else row["En"]) == 0.0 and (row['language'] == En) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aa"]) else row["aa"]) == 0.0 and (row['language'] == aa) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["ace"]) else row["ace"]) == 0.0 and (row['language'] == ace) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-arab"]) else row["aeb-arab"]) == 0.0 and (row['language'] == aeb-arab) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-latn"]) else row["aeb-latn"]) == 0.0 and (row['language'] == aeb-latn) or (row['property'] == 'P4188') and (None == 3114) and (row['language'] == index), axis=1)]
# size__after = df0.shape[0]
# print("Input data_0.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
# df0_0 = df0[df0.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P4188') and ( 0 if pd.isnull(row["En"]) else row["En"]) == 0.0 and (row['language'] == En) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aa"]) else row["aa"]) == 0.0 and (row['language'] == aa) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["ace"]) else row["ace"]) == 0.0 and (row['language'] == ace) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-arab"]) else row["aeb-arab"]) == 0.0 and (row['language'] == aeb-arab) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-latn"]) else row["aeb-latn"]) == 0.0 and (row['language'] == aeb-latn) or (row['property'] == 'P4188') and (None == 3114) and (row['language'] == index), axis=1)]
# df1 = df0_0.drop_duplicates(subset=["timestamp","property","language"])
# df1_0 = df1[df1.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P4188') and ( 0 if pd.isnull(row["En"]) else row["En"]) == 0.0 and (row['language'] == En) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aa"]) else row["aa"]) == 0.0 and (row['language'] == aa) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["ace"]) else row["ace"]) == 0.0 and (row['language'] == ace) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-arab"]) else row["aeb-arab"]) == 0.0 and (row['language'] == aeb-arab) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-latn"]) else row["aeb-latn"]) == 0.0 and (row['language'] == aeb-latn) or (row['property'] == 'P4188') and (None == 3114) and (row['language'] == index), axis=1)]
# df2 = df1_0.sort_values(by=["property","timestamp"])
# df2_0 = df2[df2.apply(lambda row: (not (pd.isnull(row['language']))) and (row['property'] == 'P4188') and ( 0 if pd.isnull(row["En"]) else row["En"]) == 0.0 and (row['language'] == En) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aa"]) else row["aa"]) == 0.0 and (row['language'] == aa) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["ace"]) else row["ace"]) == 0.0 and (row['language'] == ace) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-arab"]) else row["aeb-arab"]) == 0.0 and (row['language'] == aeb-arab) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-latn"]) else row["aeb-latn"]) == 0.0 and (row['language'] == aeb-latn) or (row['property'] == 'P4188') and (None == 3114) and (row['language'] == index), axis=1)]
# df3 = df2_0[df2_0.apply(lambda row: not (pd.isnull(row['language'])), axis=1)]
# df3_0 = df3[df3.apply(lambda row: (row['property'] == 'P4188') and ( 0 if pd.isnull(row["En"]) else row["En"]) == 0.0 and (row['language'] == En) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aa"]) else row["aa"]) == 0.0 and (row['language'] == aa) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["ace"]) else row["ace"]) == 0.0 and (row['language'] == ace) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-arab"]) else row["aeb-arab"]) == 0.0 and (row['language'] == aeb-arab) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-latn"]) else row["aeb-latn"]) == 0.0 and (row['language'] == aeb-latn) or (row['property'] == 'P4188') and (None == 3114) and (row['language'] == index), axis=1)]
# df4 = df3_0
# df4["rating"] = df4.apply(lambda xxx__: 1, axis=1)
# df4_0 = df4[df4.apply(lambda row: (row['property'] == 'P4188') and ( 0 if pd.isnull(row["En"]) else row["En"]) == 0.0 and (row['language'] == En) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aa"]) else row["aa"]) == 0.0 and (row['language'] == aa) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["ace"]) else row["ace"]) == 0.0 and (row['language'] == ace) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-arab"]) else row["aeb-arab"]) == 0.0 and (row['language'] == aeb-arab) or (row['property'] == 'P4188') and ( 0 if pd.isnull(row["aeb-latn"]) else row["aeb-latn"]) == 0.0 and (row['language'] == aeb-latn) or (row['property'] == 'P4188') and (None == 3114) and (row['language'] == index), axis=1)]
# df5 = df4_0.pivot(index="property", columns="language", values="rating").reset_index()
# df5_0 = df5[df5.apply(lambda row: (row['property'] == 'P4188') and ( 0 if pd.isnull(row["En"]) else row["En"]) == 0.0 and ( 0 if pd.isnull(row["aa"]) else row["aa"]) == 0.0 and ( 0 if pd.isnull(row["ace"]) else row["ace"]) == 0.0 and ( 0 if pd.isnull(row["aeb-arab"]) else row["aeb-arab"]) == 0.0 and ( 0 if pd.isnull(row["aeb-latn"]) else row["aeb-latn"]) == 0.0 and (row['index'] == 3114), axis=1)]
# df6=df5_0
# df6["En"] = df6["En"].fillna(value=0)
# df6_0 = df6[df6.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and ( 0 if pd.isnull(row["aa"]) else row["aa"]) == 0.0 and ( 0 if pd.isnull(row["ace"]) else row["ace"]) == 0.0 and ( 0 if pd.isnull(row["aeb-arab"]) else row["aeb-arab"]) == 0.0 and ( 0 if pd.isnull(row["aeb-latn"]) else row["aeb-latn"]) == 0.0 and (row['index'] == 3114), axis=1)]
# df7=df6_0
# df7["aa"] = df7["aa"].fillna(value=0)
# df7_0 = df7[df7.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and ( 0 if pd.isnull(row["ace"]) else row["ace"]) == 0.0 and ( 0 if pd.isnull(row["aeb-arab"]) else row["aeb-arab"]) == 0.0 and ( 0 if pd.isnull(row["aeb-latn"]) else row["aeb-latn"]) == 0.0 and (row['index'] == 3114), axis=1)]
# df8=df7_0
# df8["ace"] = df8["ace"].fillna(value=0)
# df8_0 = df8[df8.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and ( 0 if pd.isnull(row["aeb-arab"]) else row["aeb-arab"]) == 0.0 and ( 0 if pd.isnull(row["aeb-latn"]) else row["aeb-latn"]) == 0.0 and (row['index'] == 3114), axis=1)]
# df9=df8_0
# df9["aeb-arab"] = df9["aeb-arab"].fillna(value=0)
# df9_0 = df9[df9.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and ( 0 if pd.isnull(row["aeb-latn"]) else row["aeb-latn"]) == 0.0 and (row['index'] == 3114), axis=1)]
# df10=df9_0
# df10["aeb-latn"] = df10["aeb-latn"].fillna(value=0)
# df10_0 = df10[df10.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df11=df10_0
# df11["af"] = df11["af"].fillna(value=0)
# df11_0 = df11[df11.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df12=df11_0
# df12["ak"] = df12["ak"].fillna(value=0)
# df12_0 = df12[df12.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df13=df12_0
# df13["als"] = df13["als"].fillna(value=0)
# df13_0 = df13[df13.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df14=df13_0
# df14["am"] = df14["am"].fillna(value=0)
# df14_0 = df14[df14.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df15=df14_0
# df15["an"] = df15["an"].fillna(value=0)
# df15_0 = df15[df15.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df16=df15_0
# df16["ang"] = df16["ang"].fillna(value=0)
# df16_0 = df16[df16.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df17=df16_0
# df17["ar"] = df17["ar"].fillna(value=0)
# df17_0 = df17[df17.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df18=df17_0
# df18["arn"] = df18["arn"].fillna(value=0)
# df18_0 = df18[df18.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df19=df18_0
# df19["arq"] = df19["arq"].fillna(value=0)
# df19_0 = df19[df19.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df20=df19_0
# df20["ary"] = df20["ary"].fillna(value=0)
# df20_0 = df20[df20.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df21=df20_0
# df21["arz"] = df21["arz"].fillna(value=0)
# df21_0 = df21[df21.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df22=df21_0
# df22["as"] = df22["as"].fillna(value=0)
# df22_0 = df22[df22.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df23=df22_0
# df23["ast"] = df23["ast"].fillna(value=0)
# df23_0 = df23[df23.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df24=df23_0
# df24["atj"] = df24["atj"].fillna(value=0)
# df24_0 = df24[df24.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df25=df24_0
# df25["ay"] = df25["ay"].fillna(value=0)
# df25_0 = df25[df25.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df26=df25_0
# df26["az"] = df26["az"].fillna(value=0)
# df26_0 = df26[df26.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df27=df26_0
# df27["ba"] = df27["ba"].fillna(value=0)
# df27_0 = df27[df27.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df28=df27_0
# df28["bar"] = df28["bar"].fillna(value=0)
# df28_0 = df28[df28.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df29=df28_0
# df29["be"] = df29["be"].fillna(value=0)
# df29_0 = df29[df29.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df30=df29_0
# df30["be-tarask"] = df30["be-tarask"].fillna(value=0)
# df30_0 = df30[df30.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df31=df30_0
# df31["bg"] = df31["bg"].fillna(value=0)
# df31_0 = df31[df31.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df32=df31_0
# df32["bgn"] = df32["bgn"].fillna(value=0)
# df32_0 = df32[df32.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df33=df32_0
# df33["bh"] = df33["bh"].fillna(value=0)
# df33_0 = df33[df33.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df34=df33_0
# df34["bho"] = df34["bho"].fillna(value=0)
# df34_0 = df34[df34.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df35=df34_0
# df35["bn"] = df35["bn"].fillna(value=0)
# df35_0 = df35[df35.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df36=df35_0
# df36["bpy"] = df36["bpy"].fillna(value=0)
# df36_0 = df36[df36.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df37=df36_0
# df37["br"] = df37["br"].fillna(value=0)
# df37_0 = df37[df37.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df38=df37_0
# df38["bs"] = df38["bs"].fillna(value=0)
# df38_0 = df38[df38.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df39=df38_0
# df39["bxr"] = df39["bxr"].fillna(value=0)
# df39_0 = df39[df39.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df40=df39_0
# df40["ca"] = df40["ca"].fillna(value=0)
# df40_0 = df40[df40.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df41=df40_0
# df41["cbk-zam"] = df41["cbk-zam"].fillna(value=0)
# df41_0 = df41[df41.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df42=df41_0
# df42["ce"] = df42["ce"].fillna(value=0)
# df42_0 = df42[df42.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df43=df42_0
# df43["ceb"] = df43["ceb"].fillna(value=0)
# df43_0 = df43[df43.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df44=df43_0
# df44["ckb"] = df44["ckb"].fillna(value=0)
# df44_0 = df44[df44.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df45=df44_0
# df45["co"] = df45["co"].fillna(value=0)
# df45_0 = df45[df45.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df46=df45_0
# df46["cs"] = df46["cs"].fillna(value=0)
# df46_0 = df46[df46.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df47=df46_0
# df47["cy"] = df47["cy"].fillna(value=0)
# df47_0 = df47[df47.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df48=df47_0
# df48["da"] = df48["da"].fillna(value=0)
# df48_0 = df48[df48.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df49=df48_0
# df49["de"] = df49["de"].fillna(value=0)
# df49_0 = df49[df49.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df50=df49_0
# df50["de-at"] = df50["de-at"].fillna(value=0)
# df50_0 = df50[df50.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df51=df50_0
# df51["de-ch"] = df51["de-ch"].fillna(value=0)
# df51_0 = df51[df51.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df52=df51_0
# df52["de-formal"] = df52["de-formal"].fillna(value=0)
# df52_0 = df52[df52.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df53=df52_0
# df53["dsb"] = df53["dsb"].fillna(value=0)
# df53_0 = df53[df53.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df54=df53_0
# df54["dty"] = df54["dty"].fillna(value=0)
# df54_0 = df54[df54.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df55=df54_0
# df55["ee"] = df55["ee"].fillna(value=0)
# df55_0 = df55[df55.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df56=df55_0
# df56["el"] = df56["el"].fillna(value=0)
# df56_0 = df56[df56.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df57=df56_0
# df57["en"] = df57["en"].fillna(value=0)
# df57_0 = df57[df57.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df58=df57_0
# df58["en-ca"] = df58["en-ca"].fillna(value=0)
# df58_0 = df58[df58.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df59=df58_0
# df59["en-gb"] = df59["en-gb"].fillna(value=0)
# df59_0 = df59[df59.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df60=df59_0
# df60["eo"] = df60["eo"].fillna(value=0)
# df60_0 = df60[df60.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df61=df60_0
# df61["es"] = df61["es"].fillna(value=0)
# df61_0 = df61[df61.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df62=df61_0
# df62["et"] = df62["et"].fillna(value=0)
# df62_0 = df62[df62.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df63=df62_0
# df63["eu"] = df63["eu"].fillna(value=0)
# df63_0 = df63[df63.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df64=df63_0
# df64["fa"] = df64["fa"].fillna(value=0)
# df64_0 = df64[df64.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df65=df64_0
# df65["fi"] = df65["fi"].fillna(value=0)
# df65_0 = df65[df65.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df66=df65_0
# df66["fo"] = df66["fo"].fillna(value=0)
# df66_0 = df66[df66.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df67=df66_0
# df67["fr"] = df67["fr"].fillna(value=0)
# df67_0 = df67[df67.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df68=df67_0
# df68["frc"] = df68["frc"].fillna(value=0)
# df68_0 = df68[df68.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df69=df68_0
# df69["frp"] = df69["frp"].fillna(value=0)
# df69_0 = df69[df69.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df70=df69_0
# df70["frr"] = df70["frr"].fillna(value=0)
# df70_0 = df70[df70.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df71=df70_0
# df71["fur"] = df71["fur"].fillna(value=0)
# df71_0 = df71[df71.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df72=df71_0
# df72["fy"] = df72["fy"].fillna(value=0)
# df72_0 = df72[df72.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df73=df72_0
# df73["ga"] = df73["ga"].fillna(value=0)
# df73_0 = df73[df73.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df74=df73_0
# df74["gan"] = df74["gan"].fillna(value=0)
# df74_0 = df74[df74.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df75=df74_0
# df75["gd"] = df75["gd"].fillna(value=0)
# df75_0 = df75[df75.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df76=df75_0
# df76["gl"] = df76["gl"].fillna(value=0)
# df76_0 = df76[df76.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df77=df76_0
# df77["glk"] = df77["glk"].fillna(value=0)
# df77_0 = df77[df77.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df78=df77_0
# df78["gn"] = df78["gn"].fillna(value=0)
# df78_0 = df78[df78.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df79=df78_0
# df79["gsw"] = df79["gsw"].fillna(value=0)
# df79_0 = df79[df79.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df80=df79_0
# df80["gu"] = df80["gu"].fillna(value=0)
# df80_0 = df80[df80.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df81=df80_0
# df81["gv"] = df81["gv"].fillna(value=0)
# df81_0 = df81[df81.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df82=df81_0
# df82["ha"] = df82["ha"].fillna(value=0)
# df82_0 = df82[df82.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df83=df82_0
# df83["he"] = df83["he"].fillna(value=0)
# df83_0 = df83[df83.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df84=df83_0
# df84["hi"] = df84["hi"].fillna(value=0)
# df84_0 = df84[df84.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df85=df84_0
# df85["hr"] = df85["hr"].fillna(value=0)
# df85_0 = df85[df85.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df86=df85_0
# df86["hsb"] = df86["hsb"].fillna(value=0)
# df86_0 = df86[df86.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df87=df86_0
# df87["ht"] = df87["ht"].fillna(value=0)
# df87_0 = df87[df87.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df88=df87_0
# df88["hu"] = df88["hu"].fillna(value=0)
# df88_0 = df88[df88.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df89=df88_0
# df89["hy"] = df89["hy"].fillna(value=0)
# df89_0 = df89[df89.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df90=df89_0
# df90["ia"] = df90["ia"].fillna(value=0)
# df90_0 = df90[df90.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df91=df90_0
# df91["id"] = df91["id"].fillna(value=0)
# df91_0 = df91[df91.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df92=df91_0
# df92["ie"] = df92["ie"].fillna(value=0)
# df92_0 = df92[df92.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df93=df92_0
# df93["ii"] = df93["ii"].fillna(value=0)
# df93_0 = df93[df93.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df94=df93_0
# df94["ik"] = df94["ik"].fillna(value=0)
# df94_0 = df94[df94.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df95=df94_0
# df95["ilo"] = df95["ilo"].fillna(value=0)
# df95_0 = df95[df95.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df96=df95_0
# df96["io"] = df96["io"].fillna(value=0)
# df96_0 = df96[df96.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df97=df96_0
# df97["is"] = df97["is"].fillna(value=0)
# df97_0 = df97[df97.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df98=df97_0
# df98["it"] = df98["it"].fillna(value=0)
# df98_0 = df98[df98.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df99=df98_0
# df99["iu"] = df99["iu"].fillna(value=0)
# df99_0 = df99[df99.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df100=df99_0
# df100["ja"] = df100["ja"].fillna(value=0)
# df100_0 = df100[df100.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df101=df100_0
# df101["jam"] = df101["jam"].fillna(value=0)
# df101_0 = df101[df101.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df102=df101_0
# df102["jbo"] = df102["jbo"].fillna(value=0)
# df102_0 = df102[df102.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df103=df102_0
# df103["jv"] = df103["jv"].fillna(value=0)
# df103_0 = df103[df103.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df104=df103_0
# df104["ka"] = df104["ka"].fillna(value=0)
# df104_0 = df104[df104.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df105=df104_0
# df105["kg"] = df105["kg"].fillna(value=0)
# df105_0 = df105[df105.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df106=df105_0
# df106["kk"] = df106["kk"].fillna(value=0)
# df106_0 = df106[df106.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df107=df106_0
# df107["kl"] = df107["kl"].fillna(value=0)
# df107_0 = df107[df107.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df108=df107_0
# df108["kn"] = df108["kn"].fillna(value=0)
# df108_0 = df108[df108.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df109=df108_0
# df109["ko"] = df109["ko"].fillna(value=0)
# df109_0 = df109[df109.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df110=df109_0
# df110["ksh"] = df110["ksh"].fillna(value=0)
# df110_0 = df110[df110.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df111=df110_0
# df111["ku"] = df111["ku"].fillna(value=0)
# df111_0 = df111[df111.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df112=df111_0
# df112["ku-latn"] = df112["ku-latn"].fillna(value=0)
# df112_0 = df112[df112.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df113=df112_0
# df113["ky"] = df113["ky"].fillna(value=0)
# df113_0 = df113[df113.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df114=df113_0
# df114["la"] = df114["la"].fillna(value=0)
# df114_0 = df114[df114.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df115=df114_0
# df115["lb"] = df115["lb"].fillna(value=0)
# df115_0 = df115[df115.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df116=df115_0
# df116["lfn"] = df116["lfn"].fillna(value=0)
# df116_0 = df116[df116.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df117=df116_0
# df117["li"] = df117["li"].fillna(value=0)
# df117_0 = df117[df117.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df118=df117_0
# df118["lij"] = df118["lij"].fillna(value=0)
# df118_0 = df118[df118.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df119=df118_0
# df119["lmo"] = df119["lmo"].fillna(value=0)
# df119_0 = df119[df119.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df120=df119_0
# df120["ln"] = df120["ln"].fillna(value=0)
# df120_0 = df120[df120.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df121=df120_0
# df121["lo"] = df121["lo"].fillna(value=0)
# df121_0 = df121[df121.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df122=df121_0
# df122["lt"] = df122["lt"].fillna(value=0)
# df122_0 = df122[df122.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df123=df122_0
# df123["lv"] = df123["lv"].fillna(value=0)
# df123_0 = df123[df123.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df124=df123_0
# df124["mai"] = df124["mai"].fillna(value=0)
# df124_0 = df124[df124.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df125=df124_0
# df125["mg"] = df125["mg"].fillna(value=0)
# df125_0 = df125[df125.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df126=df125_0
# df126["mi"] = df126["mi"].fillna(value=0)
# df126_0 = df126[df126.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df127=df126_0
# df127["min"] = df127["min"].fillna(value=0)
# df127_0 = df127[df127.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df128=df127_0
# df128["mk"] = df128["mk"].fillna(value=0)
# df128_0 = df128[df128.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df129=df128_0
# df129["ml"] = df129["ml"].fillna(value=0)
# df129_0 = df129[df129.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df130=df129_0
# df130["mn"] = df130["mn"].fillna(value=0)
# df130_0 = df130[df130.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df131=df130_0
# df131["mr"] = df131["mr"].fillna(value=0)
# df131_0 = df131[df131.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df132=df131_0
# df132["ms"] = df132["ms"].fillna(value=0)
# df132_0 = df132[df132.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df133=df132_0
# df133["mt"] = df133["mt"].fillna(value=0)
# df133_0 = df133[df133.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df134=df133_0
# df134["mwl"] = df134["mwl"].fillna(value=0)
# df134_0 = df134[df134.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df135=df134_0
# df135["my"] = df135["my"].fillna(value=0)
# df135_0 = df135[df135.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df136=df135_0
# df136["myv"] = df136["myv"].fillna(value=0)
# df136_0 = df136[df136.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df137=df136_0
# df137["mzn"] = df137["mzn"].fillna(value=0)
# df137_0 = df137[df137.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df138=df137_0
# df138["nap"] = df138["nap"].fillna(value=0)
# df138_0 = df138[df138.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df139=df138_0
# df139["nb"] = df139["nb"].fillna(value=0)
# df139_0 = df139[df139.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df140=df139_0
# df140["nds"] = df140["nds"].fillna(value=0)
# df140_0 = df140[df140.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df141=df140_0
# df141["nds-nl"] = df141["nds-nl"].fillna(value=0)
# df141_0 = df141[df141.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df142=df141_0
# df142["ne"] = df142["ne"].fillna(value=0)
# df142_0 = df142[df142.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df143=df142_0
# df143["nl"] = df143["nl"].fillna(value=0)
# df143_0 = df143[df143.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df144=df143_0
# df144["nn"] = df144["nn"].fillna(value=0)
# df144_0 = df144[df144.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df145=df144_0
# df145["no"] = df145["no"].fillna(value=0)
# df145_0 = df145[df145.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df146=df145_0
# df146["nov"] = df146["nov"].fillna(value=0)
# df146_0 = df146[df146.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df147=df146_0
# df147["nrm"] = df147["nrm"].fillna(value=0)
# df147_0 = df147[df147.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df148=df147_0
# df148["nso"] = df148["nso"].fillna(value=0)
# df148_0 = df148[df148.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df149=df148_0
# df149["oc"] = df149["oc"].fillna(value=0)
# df149_0 = df149[df149.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df150=df149_0
# df150["olo"] = df150["olo"].fillna(value=0)
# df150_0 = df150[df150.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df151=df150_0
# df151["or"] = df151["or"].fillna(value=0)
# df151_0 = df151[df151.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df152=df151_0
# df152["pa"] = df152["pa"].fillna(value=0)
# df152_0 = df152[df152.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df153=df152_0
# df153["pam"] = df153["pam"].fillna(value=0)
# df153_0 = df153[df153.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df154=df153_0
# df154["pcd"] = df154["pcd"].fillna(value=0)
# df154_0 = df154[df154.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df155=df154_0
# df155["pdc"] = df155["pdc"].fillna(value=0)
# df155_0 = df155[df155.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df156=df155_0
# df156["pfl"] = df156["pfl"].fillna(value=0)
# df156_0 = df156[df156.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df157=df156_0
# df157["pi"] = df157["pi"].fillna(value=0)
# df157_0 = df157[df157.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df158=df157_0
# df158["pl"] = df158["pl"].fillna(value=0)
# df158_0 = df158[df158.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df159=df158_0
# df159["pms"] = df159["pms"].fillna(value=0)
# df159_0 = df159[df159.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df160=df159_0
# df160["ps"] = df160["ps"].fillna(value=0)
# df160_0 = df160[df160.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df161=df160_0
# df161["pt"] = df161["pt"].fillna(value=0)
# df161_0 = df161[df161.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df162=df161_0
# df162["pt-br"] = df162["pt-br"].fillna(value=0)
# df162_0 = df162[df162.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df163=df162_0
# df163["qu"] = df163["qu"].fillna(value=0)
# df163_0 = df163[df163.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df164=df163_0
# df164["rm"] = df164["rm"].fillna(value=0)
# df164_0 = df164[df164.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df165=df164_0
# df165["ro"] = df165["ro"].fillna(value=0)
# df165_0 = df165[df165.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df166=df165_0
# df166["roa-tara"] = df166["roa-tara"].fillna(value=0)
# df166_0 = df166[df166.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df167=df166_0
# df167["ru"] = df167["ru"].fillna(value=0)
# df167_0 = df167[df167.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df168=df167_0
# df168["sa"] = df168["sa"].fillna(value=0)
# df168_0 = df168[df168.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df169=df168_0
# df169["sah"] = df169["sah"].fillna(value=0)
# df169_0 = df169[df169.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df170=df169_0
# df170["sc"] = df170["sc"].fillna(value=0)
# df170_0 = df170[df170.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df171=df170_0
# df171["scn"] = df171["scn"].fillna(value=0)
# df171_0 = df171[df171.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df172=df171_0
# df172["sco"] = df172["sco"].fillna(value=0)
# df172_0 = df172[df172.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df173=df172_0
# df173["sd"] = df173["sd"].fillna(value=0)
# df173_0 = df173[df173.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df174=df173_0
# df174["se"] = df174["se"].fillna(value=0)
# df174_0 = df174[df174.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df175=df174_0
# df175["sei"] = df175["sei"].fillna(value=0)
# df175_0 = df175[df175.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df176=df175_0
# df176["sgs"] = df176["sgs"].fillna(value=0)
# df176_0 = df176[df176.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df177=df176_0
# df177["sh"] = df177["sh"].fillna(value=0)
# df177_0 = df177[df177.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df178=df177_0
# df178["shi"] = df178["shi"].fillna(value=0)
# df178_0 = df178[df178.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df179=df178_0
# df179["shi-tfng"] = df179["shi-tfng"].fillna(value=0)
# df179_0 = df179[df179.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df180=df179_0
# df180["si"] = df180["si"].fillna(value=0)
# df180_0 = df180[df180.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df181=df180_0
# df181["simple"] = df181["simple"].fillna(value=0)
# df181_0 = df181[df181.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df182=df181_0
# df182["sk"] = df182["sk"].fillna(value=0)
# df182_0 = df182[df182.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df183=df182_0
# df183["skr-arab"] = df183["skr-arab"].fillna(value=0)
# df183_0 = df183[df183.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df184=df183_0
# df184["sl"] = df184["sl"].fillna(value=0)
# df184_0 = df184[df184.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df185=df184_0
# df185["sma"] = df185["sma"].fillna(value=0)
# df185_0 = df185[df185.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df186=df185_0
# df186["smj"] = df186["smj"].fillna(value=0)
# df186_0 = df186[df186.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df187=df186_0
# df187["sq"] = df187["sq"].fillna(value=0)
# df187_0 = df187[df187.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df188=df187_0
# df188["sr"] = df188["sr"].fillna(value=0)
# df188_0 = df188[df188.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df189=df188_0
# df189["sr-ec"] = df189["sr-ec"].fillna(value=0)
# df189_0 = df189[df189.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df190=df189_0
# df190["sr-el"] = df190["sr-el"].fillna(value=0)
# df190_0 = df190[df190.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df191=df190_0
# df191["stq"] = df191["stq"].fillna(value=0)
# df191_0 = df191[df191.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df192=df191_0
# df192["su"] = df192["su"].fillna(value=0)
# df192_0 = df192[df192.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df193=df192_0
# df193["sv"] = df193["sv"].fillna(value=0)
# df193_0 = df193[df193.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df194=df193_0
# df194["sw"] = df194["sw"].fillna(value=0)
# df194_0 = df194[df194.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df195=df194_0
# df195["szl"] = df195["szl"].fillna(value=0)
# df195_0 = df195[df195.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df196=df195_0
# df196["ta"] = df196["ta"].fillna(value=0)
# df196_0 = df196[df196.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df197=df196_0
# df197["tcy"] = df197["tcy"].fillna(value=0)
# df197_0 = df197[df197.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df198=df197_0
# df198["te"] = df198["te"].fillna(value=0)
# df198_0 = df198[df198.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df199=df198_0
# df199["tg"] = df199["tg"].fillna(value=0)
# df199_0 = df199[df199.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df200=df199_0
# df200["tg-cyrl"] = df200["tg-cyrl"].fillna(value=0)
# df200_0 = df200[df200.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df201=df200_0
# df201["th"] = df201["th"].fillna(value=0)
# df201_0 = df201[df201.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df202=df201_0
# df202["tl"] = df202["tl"].fillna(value=0)
# df202_0 = df202[df202.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df203=df202_0
# df203["tokipona"] = df203["tokipona"].fillna(value=0)
# df203_0 = df203[df203.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df204=df203_0
# df204["tr"] = df204["tr"].fillna(value=0)
# df204_0 = df204[df204.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df205=df204_0
# df205["ts"] = df205["ts"].fillna(value=0)
# df205_0 = df205[df205.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df206=df205_0
# df206["tt"] = df206["tt"].fillna(value=0)
# df206_0 = df206[df206.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df207=df206_0
# df207["tt-cyrl"] = df207["tt-cyrl"].fillna(value=0)
# df207_0 = df207[df207.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df208=df207_0
# df208["tt-latn"] = df208["tt-latn"].fillna(value=0)
# df208_0 = df208[df208.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df209=df208_0
# df209["ty"] = df209["ty"].fillna(value=0)
# df209_0 = df209[df209.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df210=df209_0
# df210["udm"] = df210["udm"].fillna(value=0)
# df210_0 = df210[df210.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df211=df210_0
# df211["uk"] = df211["uk"].fillna(value=0)
# df211_0 = df211[df211.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df212=df211_0
# df212["ur"] = df212["ur"].fillna(value=0)
# df212_0 = df212[df212.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df213=df212_0
# df213["uz"] = df213["uz"].fillna(value=0)
# df213_0 = df213[df213.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df214=df213_0
# df214["vec"] = df214["vec"].fillna(value=0)
# df214_0 = df214[df214.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df215=df214_0
# df215["vi"] = df215["vi"].fillna(value=0)
# df215_0 = df215[df215.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df216=df215_0
# df216["vls"] = df216["vls"].fillna(value=0)
# df216_0 = df216[df216.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df217=df216_0
# df217["vmf"] = df217["vmf"].fillna(value=0)
# df217_0 = df217[df217.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df218=df217_0
# df218["vo"] = df218["vo"].fillna(value=0)
# df218_0 = df218[df218.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df219=df218_0
# df219["wa"] = df219["wa"].fillna(value=0)
# df219_0 = df219[df219.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df220=df219_0
# df220["war"] = df220["war"].fillna(value=0)
# df220_0 = df220[df220.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df221=df220_0
# df221["wo"] = df221["wo"].fillna(value=0)
# df221_0 = df221[df221.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df222=df221_0
# df222["wuu"] = df222["wuu"].fillna(value=0)
# df222_0 = df222[df222.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df223=df222_0
# df223["xh"] = df223["xh"].fillna(value=0)
# df223_0 = df223[df223.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df224=df223_0
# df224["yi"] = df224["yi"].fillna(value=0)
# df224_0 = df224[df224.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df225=df224_0
# df225["yo"] = df225["yo"].fillna(value=0)
# df225_0 = df225[df225.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df226=df225_0
# df226["yue"] = df226["yue"].fillna(value=0)
# df226_0 = df226[df226.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df227=df226_0
# df227["za"] = df227["za"].fillna(value=0)
# df227_0 = df227[df227.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df228=df227_0
# df228["zea"] = df228["zea"].fillna(value=0)
# df228_0 = df228[df228.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df229=df228_0
# df229["zh"] = df229["zh"].fillna(value=0)
# df229_0 = df229[df229.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df230=df229_0
# df230["zh-cn"] = df230["zh-cn"].fillna(value=0)
# df230_0 = df230[df230.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df231=df230_0
# df231["zh-hans"] = df231["zh-hans"].fillna(value=0)
# df231_0 = df231[df231.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df232=df231_0
# df232["zh-hant"] = df232["zh-hant"].fillna(value=0)
# df232_0 = df232[df232.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df233=df232_0
# df233["zh-hk"] = df233["zh-hk"].fillna(value=0)
# df233_0 = df233[df233.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df234=df233_0
# df234["zh-mo"] = df234["zh-mo"].fillna(value=0)
# df234_0 = df234[df234.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df235=df234_0
# df235["zh-my"] = df235["zh-my"].fillna(value=0)
# df235_0 = df235[df235.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df236=df235_0
# df236["zh-sg"] = df236["zh-sg"].fillna(value=0)
# df236_0 = df236[df236.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df237=df236_0
# df237["zh-tw"] = df237["zh-tw"].fillna(value=0)
# df237_0 = df237[df237.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df238=df237_0
# df238["zh-yue"] = df238["zh-yue"].fillna(value=0)
# df238_0 = df238[df238.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# df239=df238_0
# df239["zu"] = df239["zu"].fillna(value=0)
# df239_0 = df239[df239.apply(lambda row: (row['property'] == 'P4188') and (row['En'] == 0.0) and (row['aa'] == 0.0) and (row['ace'] == 0.0) and (row['aeb-arab'] == 0.0) and (row['aeb-latn'] == 0.0) and (row['index'] == 3114), axis=1)]
# pickle.dump(df239, open('temp//result_after_pushdown.p', 'wb'))