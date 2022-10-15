import pickle
# df0 = pickle.load(open("data_0.pickle",'rb'))
# df0.columns = [str(c) for c in df0.columns]
# df0["index"] = df0.index
# df1=df0.rename(columns={ "{8355F008-C0A9-55C5-E053-6B04A8C0D090}":"TUID","102000":"Price","2003-11-25 00:00":"Date_Transfer","WA5 2PG":"Postcode","S":"Prop_Type","N":"Old_New","L":"Duration","39":"PAON","Unnamed: 8":"SAON","ROTHAY DRIVE":"Street","PENKETH":"Locality","WARRINGTON":"Town_City","WARRINGTON.1":"District","WARRINGTON.2":"County","A":"PPD_Cat_Type","A.1":"Record_Status" })
# df2 = df1.sort_values(by=["Date_Transfer"])
# df3 = df2[df2.apply(lambda row: row['Town_City'] == 'LONDON', axis=1)]
# df4 = df3.groupby(["Street"]).agg({ "Price":"mean" }).reset_index().rename(columns={ "Price":"Price" })
# final_output = df4[df4.apply(lambda row: ((row['Price'] >= 2000000) and (row['Price'] <= 3000000)) and ((row['Street'] == 'NORTH SQUARE') or (row['Price'] == 2500000)), axis=1)]
# print(final_output)
# pickle.dump(final_output, open('temp//result_pred_on_output.p', 'wb'))

df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df1=df0.rename(columns={ "{8355F008-C0A9-55C5-E053-6B04A8C0D090}":"TUID","102000":"Price","2003-11-25 00:00":"Date_Transfer","WA5 2PG":"Postcode","S":"Prop_Type","N":"Old_New","L":"Duration","39":"PAON","Unnamed: 8":"SAON","ROTHAY DRIVE":"Street","PENKETH":"Locality","WARRINGTON":"Town_City","WARRINGTON.1":"District","WARRINGTON.2":"County","A":"PPD_Cat_Type","A.1":"Record_Status" })
df2 = df1.sort_values(by=["Date_Transfer"])
df3 = df2[df2.apply(lambda row: row['Town_City'] == 'LONDON', axis=1)]
df4 = df3.groupby(["Street"]).agg({ "Price":"mean" }).reset_index().rename(columns={ "Price":"Price" })
df5=df4.rename(columns={ "Street":"Street","Price":"Avg_Price" })
df6 = df5[df5.apply(lambda row: (row['Avg_Price'] >= 2000000) and (row['Avg_Price'] <= 3000000), axis=1)]
final_output = df6[df6.apply(lambda row: None, axis=1)]
pickle.dump(final_output, open('temp//result_pred_on_output.p', 'wb'))