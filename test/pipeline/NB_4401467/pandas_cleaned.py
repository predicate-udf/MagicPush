import pandas as pd
import pickle
df0=pickle.load(open('data_0.pickle','rb'))
print(df0.columns)
df2 = df0[df0["language"] == "english"]
df6 = df2[df2["country"] == "US"]
df7 = df6[["doc_id","author","published","title","text","thread_title","type"]]
df7["word_count"] = df7.apply(lambda xxx__: len(xxx__["text"].split(" ")), axis=1)
df7["published"] = df7.apply(lambda xxx__: xxx__["published"][0:10], axis=1)
df15 = df7[df7["published"].isin(["2016-10-26","2016-10-27"])]
df18 = df15.drop_duplicates(subset=["title","text"])
df21 = df18[df18["word_count"] > 200]
df24 = df21[df21["title"].notnull()]
df25 = df24.reset_index()

df41 = df25.dropna(subset=["text","author"])
df42 = df41.drop_duplicates(subset=["text"])

