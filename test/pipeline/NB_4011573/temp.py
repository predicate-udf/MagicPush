import pandas as pd
import numpy as py
import pickle


df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
print(df0.columns)
df0 = df0[df0.apply(lambda row: (row['1'] == 22046) and ( str(row['2']).replace('\n', '').replace('\r', '')) == 'MYSORE MED.& RESEARCHINST. MYSORE' and (row['3'] == 'MBBS') and (row['6:'] == np.nan) and (row['7'] == np.nan) and (row['0'] == 5238), axis=1)]
df0_0 = df0[df0.apply(lambda row: (row['1'] == 22046) and ( str(row['2']).replace('\n', '').replace('\r', '')) == 'MYSORE MED.& RESEARCHINST. MYSORE' and (row['3'] == 'MBBS') and (row['6:'] == np.nan) and (row['7'] == np.nan) and (row['0'] == 5238), axis=1)]
df0_0['index'] = df0_0['0']
df1=df0_0.drop(columns=['0'])
df1_0 = df1[df1.apply(lambda row: (row['1'] == 22046) and ( str(row['2']).replace('\n', '').replace('\r', '')) == 'MYSORE MED.& RESEARCHINST. MYSORE' and (row['3'] == 'MBBS') and (row['6:'] == np.nan) and (row['7'] == np.nan) and (row['index'] == 5238), axis=1)]
df2 = df1_0.drop(columns=["4","9"])
df2_0 = df2[df2.apply(lambda row: (row['1'] == 22046) and ( str(row['2']).replace('\n', '').replace('\r', '')) == 'MYSORE MED.& RESEARCHINST. MYSORE' and (row['3'] == 'MBBS') and (row['6:'] == np.nan) and (row['7'] == np.nan) and (row['index'] == 5238), axis=1)]
df3 = df2_0.drop(columns=["5","8"])
df3_0 = df3[df3.apply(lambda row: (row['1'] == 22046) and ( str(row['2']).replace('\n', '').replace('\r', '')) == 'MYSORE MED.& RESEARCHINST. MYSORE' and (row['3'] == 'MBBS') and (row['6:'] == np.nan) and (row['7'] == np.nan) and (row['index'] == 5238), axis=1)]
df4 = pickle.load(open("data_28.pickle",'rb'))
df4.columns = [str(c) for c in df4.columns]
df4["index"] = df4.index
df4 = df4[df4.apply(lambda row: (row['1'] == 22046) and (True) and (True) and (True) and (True) and (True), axis=1)]
df4_0 = df4[df4.apply(lambda row: (row['1'] == 22046) and (True) and (True) and (True) and (True) and (True), axis=1)]
df4_0['index'] = df4_0['0']
df5=df4_0.drop(columns=['0'])
df5_0 = df5[df5.apply(lambda row: (row['1'] == 22046) and (True) and (True) and (True) and (True) and (True), axis=1)]
df6 = df5_0.drop(columns=["6"])
df6_0 = df6[df6.apply(lambda row: (row['1'] == 22046) and (True) and (True) and (True) and (True) and (True), axis=1)]
df7=df6_0.rename(columns={ "1":"Id","2":"College","3":"Course","4":"6","5":"7" })
df7_0 = df7[df7.apply(lambda row: (row['Id'] == 22046) and (True) and (True) and (True) and (True) and (True), axis=1)]
df8=df3_0.rename(columns={ "1":"Id","2":"College","3":"Course","6:":"6","7":"7" })
df8_0 = df8[df8.apply(lambda row: (row['Id'] == 22046) and ( str(row['College']).replace('\n', '').replace('\r', '')) == 'MYSORE MED.& RESEARCHINST. MYSORE' and (row['Course'] == 'MBBS') and (row['6'] == np.nan) and (row['7'] == np.nan) and (row['index'] == 5238), axis=1)]
df9 = df8_0.merge(df7_0, how='left', left_on = ["Id"], right_on = ["Id"])
df9_0 = df9[df9.apply(lambda row: (row['Id'] == 22046) and ( str(row['College_x']).replace('\n','').replace('\r','') ) == 'MYSORE MED.& RESEARCHINST. MYSORE' and (row['Course_x'] == 'MBBS') and (row['6_x'] == np.nan) and (row['7_x'] == np.nan) and (row['index_x'] == 5238), axis=1)]
df10 = df9_0
df10["College_x"] = df10.apply(lambda xxx__: str(xxx__['College_x']).replace('\n','').replace('\r','') , axis=1)
df10_0 = df10[df10.apply(lambda row: (row['Id'] == 22046) and (row['College_x'] == 'MYSORE MED.& RESEARCHINST. MYSORE') and (row['Course_x'] == 'MBBS') and (row['6_x'] == np.nan) and (row['7_x'] == np.nan) and (row['index_x'] == 5238), axis=1)]
df11 = df10_0
df11["College_y"] = df11.apply(lambda xxx__: str(xxx__['College_y']).replace('\n','').replace('\r','') , axis=1)
df11_0 = df11[df11.apply(lambda row: (row['Id'] == 22046) and (row['College_x'] == 'MYSORE MED.& RESEARCHINST. MYSORE') and (row['Course_x'] == 'MBBS') and (row['6_x'] == np.nan) and (row['7_x'] == np.nan) and (row['index_x'] == 5238), axis=1)]
pickle.dump(df11, open('temp//result_after_pushdown.p', 'wb'))
