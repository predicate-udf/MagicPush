import pandas as pd
import pickle
df3=pickle.load(open('data_0.pickle','rb'))

df4 = df3[["body"]]
df5 = df4.dropna(subset=["body"])
df6 = df3[["comments"]]

df11 = df3[["article"]]
df12 = df11.dropna(subset=["article"])
df16=pickle.load(open('data_13.pickle','rb'))

df17 = df16[["body"]]
df18 = df17.dropna(subset=["body"])
df19 = df16[["comments"]]

df24 = df16[["article"]]
df25 = df24.dropna(subset=["article"])
df12.rename(columns={ "article":"text" }, inplace=True)
df6.rename(columns={ "comments":"text" }, inplace=True)
df5.rename(columns={ "body":"text" }, inplace=True)
df25.rename(columns={ "article":"text" }, inplace=True)
df19.rename(columns={ "comments":"text" }, inplace=True)
df18.rename(columns={ "body":"text" }, inplace=True)
df20 = pd.concat([df12,df6,df5,df25,df19,df18], axis=0)

# dataTS = pd.read_csv('t140.csv')
# positive = dataTS.loc[dataTS['sentiment']==4]
# positiveWithJustText = positive[['text','sentiment']]


