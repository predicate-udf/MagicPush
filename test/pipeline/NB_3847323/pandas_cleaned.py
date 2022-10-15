import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
print(len(df0))
df0.rename(columns={ "{8355F008-C0A9-55C5-E053-6B04A8C0D090}":"TUID","102000":"Price","2003-11-25 00:00":"Date_Transfer","WA5 2PG":"Postcode","S":"Prop_Type","N":"Old_New","L":"Duration","39":"PAON","Unnamed: 8":"SAON","ROTHAY DRIVE":"Street","PENKETH":"Locality","WARRINGTON":"Town_City","WARRINGTON.1":"District","WARRINGTON.2":"County","A":"PPD_Cat_Type","A.1":"Record_Status" }, inplace=True)
df0.sort_values(["Date_Transfer"], inplace=True)
df10 = df0[df0['Town_City'] == 'LONDON']
df17 = df10.groupby(by=["Street"])["Price"].mean().reset_index()

df17.rename(columns={ "Street":"Street","Price":"Avg_Price" }, inplace=True)
df18 = df17[(df17['Avg_Price'] >= 2000000) & (df17['Avg_Price'] <= 3000000)]

print(df18)