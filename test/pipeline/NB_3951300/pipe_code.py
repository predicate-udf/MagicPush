import pickle
df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
size__before = df0.shape[0]
print(df0)
df0 = df0[df0.apply(lambda row: ( row["Country/Region"].lstrip()) != 'Others' and ( row["Country/Region"].lstrip()) != 'Diamond Princess' and ( row["Country/Region"].lstrip()) != 'MS Zaandam' and ( row["Country/Region"].lstrip()) != 'Kosovo' and ( row["Country/Region"].lstrip()) != 'Holy See' and ( row["Country/Region"].lstrip()) != 'Vatican City' and ( row["Country/Region"].lstrip()) != 'Timor-Leste' and ( row["Country/Region"].lstrip()) != 'East Timor' and ( row["Country/Region"].lstrip()) != 'Channel Islands' and ( row["Country/Region"].lstrip()) != 'Western Sahara' and (True) and (row['ObservationDate'] == '03/24/2020') and ( row["Country/Region"].lstrip()) == 'Burkina Faso' and (True), axis=1)]
size__after = df0.shape[0]
print("Input data_0.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
df0_0 = df0[df0.apply(lambda row: ( row["country/region"].lstrip()) != 'Others' and ( row["country/region"].lstrip()) != 'Diamond Princess' and ( row["country/region"].lstrip()) != 'MS Zaandam' and ( row["country/region"].lstrip()) != 'Kosovo' and ( row["country/region"].lstrip()) != 'Holy See' and ( row["country/region"].lstrip()) != 'Vatican City' and ( row["country/region"].lstrip()) != 'Timor-Leste' and ( row["country/region"].lstrip()) != 'East Timor' and ( row["country/region"].lstrip()) != 'Channel Islands' and ( row["country/region"].lstrip()) != 'Western Sahara' and (True) and (row['ObservationDate'] == '03/24/2020') and ( row["country/region"].lstrip()) == 'Burkina Faso' and (True), axis=1)]
df1=df0_0.rename(columns={ "SNo":"sno","ObservationDate":"observationdate","Province/State":"province/state","Country/Region":"country/region","Last Update":"last update","Confirmed":"confirmed","Deaths":"deaths","Recovered":"recovered" })
df1_0 = df1[df1.apply(lambda row: ( row["country/region"].lstrip()) != 'Others' and ( row["country/region"].lstrip()) != 'Diamond Princess' and ( row["country/region"].lstrip()) != 'MS Zaandam' and ( row["country/region"].lstrip()) != 'Kosovo' and ( row["country/region"].lstrip()) != 'Holy See' and ( row["country/region"].lstrip()) != 'Vatican City' and ( row["country/region"].lstrip()) != 'Timor-Leste' and ( row["country/region"].lstrip()) != 'East Timor' and ( row["country/region"].lstrip()) != 'Channel Islands' and ( row["country/region"].lstrip()) != 'Western Sahara' and (True) and (row['observationdate'] == '03/24/2020') and ( row["country/region"].lstrip()) == 'Burkina Faso' and (True), axis=1)]
df2=df1_0.rename(columns={ "sno":"sno","observationdate":"observationdate","province/state":"province/state","country/region":"country/region","last update":"last_update","confirmed":"confirmed","deaths":"deaths","recovered":"recovered" })
df2_0 = df2[df2.apply(lambda row: ( row["country/region"].lstrip()) != 'Others' and ( row["country/region"].lstrip()) != 'Diamond Princess' and ( row["country/region"].lstrip()) != 'MS Zaandam' and ( row["country/region"].lstrip()) != 'Kosovo' and ( row["country/region"].lstrip()) != 'Holy See' and ( row["country/region"].lstrip()) != 'Vatican City' and ( row["country/region"].lstrip()) != 'Timor-Leste' and ( row["country/region"].lstrip()) != 'East Timor' and ( row["country/region"].lstrip()) != 'Channel Islands' and ( row["country/region"].lstrip()) != 'Western Sahara' and (True) and (row['observationdate'] == '03/24/2020') and ( row["country/region"].lstrip()) == 'Burkina Faso' and (True), axis=1)]
df3 = df2_0
df3["country/region"] = df3.apply(lambda xxx__: xxx__["country/region"].lstrip(), axis=1)
df3_0 = df3[df3.apply(lambda row: (row['country/region'] != 'Others') and (row['country/region'] != 'Diamond Princess') and (row['country/region'] != 'MS Zaandam') and (row['country/region'] != 'Kosovo') and (row['country/region'] != 'Holy See') and (row['country/region'] != 'Vatican City') and (row['country/region'] != 'Timor-Leste') and (row['country/region'] != 'East Timor') and (row['country/region'] != 'Channel Islands') and (row['country/region'] != 'Western Sahara') and (True) and (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (True), axis=1)]
df4 = df3_0[df3_0.apply(lambda row: (row['country/region'] != 'Others') and (row['country/region'] != 'Diamond Princess'), axis=1)]
df4_0 = df4[df4.apply(lambda row: (row['country/region'] != 'MS Zaandam') and (row['country/region'] != 'Kosovo') and (row['country/region'] != 'Holy See') and (row['country/region'] != 'Vatican City') and (row['country/region'] != 'Timor-Leste') and (row['country/region'] != 'East Timor') and (row['country/region'] != 'Channel Islands') and (row['country/region'] != 'Western Sahara') and (True) and (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (True), axis=1)]
df5 = df4_0[df4_0.apply(lambda row: row['country/region'] != 'MS Zaandam', axis=1)]
df5_0 = df5[df5.apply(lambda row: (row['country/region'] != 'Kosovo') and (row['country/region'] != 'Holy See') and (row['country/region'] != 'Vatican City') and (row['country/region'] != 'Timor-Leste') and (row['country/region'] != 'East Timor') and (row['country/region'] != 'Channel Islands') and (row['country/region'] != 'Western Sahara') and (True) and (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (True), axis=1)]
df6 = df5_0[df5_0.apply(lambda row: row['country/region'] != 'Kosovo', axis=1)]
df6_0 = df6[df6.apply(lambda row: (row['country/region'] != 'Holy See') and (row['country/region'] != 'Vatican City') and (row['country/region'] != 'Timor-Leste') and (row['country/region'] != 'East Timor') and (row['country/region'] != 'Channel Islands') and (row['country/region'] != 'Western Sahara') and (True) and (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (True), axis=1)]
df7 = df6_0[df6_0.apply(lambda row: row['country/region'] != 'Holy See', axis=1)]
df7_0 = df7[df7.apply(lambda row: (row['country/region'] != 'Vatican City') and (row['country/region'] != 'Timor-Leste') and (row['country/region'] != 'East Timor') and (row['country/region'] != 'Channel Islands') and (row['country/region'] != 'Western Sahara') and (True) and (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (True), axis=1)]
df8 = df7_0[df7_0.apply(lambda row: row['country/region'] != 'Vatican City', axis=1)]
df8_0 = df8[df8.apply(lambda row: (row['country/region'] != 'Timor-Leste') and (row['country/region'] != 'East Timor') and (row['country/region'] != 'Channel Islands') and (row['country/region'] != 'Western Sahara') and (True) and (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (True), axis=1)]
df9 = df8_0[df8_0.apply(lambda row: row['country/region'] != 'Timor-Leste', axis=1)]
df9_0 = df9[df9.apply(lambda row: (row['country/region'] != 'East Timor') and (row['country/region'] != 'Channel Islands') and (row['country/region'] != 'Western Sahara') and (True) and (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (True), axis=1)]
df10 = df9_0[df9_0.apply(lambda row: row['country/region'] != 'East Timor', axis=1)]
df10_0 = df10[df10.apply(lambda row: (row['country/region'] != 'Channel Islands') and (row['country/region'] != 'Western Sahara') and (True) and (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (True), axis=1)]
df11 = df10_0[df10_0.apply(lambda row: row['country/region'] != 'Channel Islands', axis=1)]
df11_0 = df11[df11.apply(lambda row: (row['country/region'] != 'Western Sahara') and (True) and (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (True), axis=1)]
df12 = df11_0[df11_0.apply(lambda row: row['country/region'] != 'Western Sahara', axis=1)]
df12_0 = df12[df12.apply(lambda row: (True) and (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (True), axis=1)]
df13 = df12_0.groupby(["observationdate","country/region"]).agg({ "confirmed":"sum" }).reset_index().rename(columns={ "confirmed":"confirmed" })
df13_0 = df13[df13.apply(lambda row: (row['confirmed'] >= 100) and (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (row['confirmed'] == 114.0), axis=1)]
df14 = df13_0[df13_0.apply(lambda row: row['confirmed'] >= 100, axis=1)]
df14_0 = df14[df14.apply(lambda row: (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (row['confirmed'] == 114.0), axis=1)]
df15 = df14_0.sort_values(by=["observationdate"])
df15_0 = df15[df15.apply(lambda row: (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (row['confirmed'] == 114.0), axis=1)]
df16 = df15_0.drop_duplicates(subset=["country/region"])
df16_0 = df16[df16.apply(lambda row: (row['observationdate'] == '03/24/2020') and (row['country/region'] == 'Burkina Faso') and (row['confirmed'] == 114.0), axis=1)]
pickle.dump(df16, open('temp//result_after_pushdown.p', 'wb'))