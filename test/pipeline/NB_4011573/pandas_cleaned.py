import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df0.rename(columns={0:'0',1:'1',2:'2',3:'3',4:'4',5:'5',6:'6',7:'7',8:'8',9:'9'},inplace=True)
df1 = df0.set_index(keys="0")
df26 = df1.drop(columns=["4","9"])
df27 = df26.drop(columns=["5","8"])
df28=pickle.load(open('data_28.pickle','rb'))
df28.rename(columns={0:'0',1:'1',2:'2',3:'3',4:'4',5:'5',6:'6'},inplace=True)
df29 = df28.set_index(keys="0")
df30 = df29.drop(columns=["6"])
df30.rename(columns={ "1":"Id","2":"College","3":"Course","4":"6","5":"7" }, inplace=True)
df27.rename(columns={ "1":"Id","2":"College","3":"Course", '6:':'6','7':'7'}, inplace=True)
df32 = df27.merge(df30, how='outer',on='Id')
df32["College_x"] = df32.apply(lambda xxx__: str(xxx__['College_x']).replace('\n','').replace('\r',''), axis=1)
df32["College_y"] = df32.apply(lambda xxx__: str(xxx__['College_y']).replace('\n','').replace('\r',''), axis=1)


print(df0)
print(df28)
print(df32)