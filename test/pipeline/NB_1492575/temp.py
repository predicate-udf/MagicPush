import pandas as pd
import pickle

df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df0 = df0[df0.apply(lambda row: (row['0'] == 30.0) and (True) and (True) and (True) and (True) and (True) or (row['0'] == 30.0) and (row['1'] == 43.0) and (row['2'] == -0.0013) and (row['3'] == -0.0002) and (row['4'] == 100.0) and (row['5'] == 518.67), axis=1)]
df0_0 = df0[df0.apply(lambda row: (row['0'] == 30.0) and (True) and (True) and (True) and (True) and (True) or (row['0'] == 30.0) and (row['1'] == 43.0) and (row['2'] == -0.0013) and (row['3'] == -0.0002) and (row['4'] == 100.0) and (row['5'] == 518.67), axis=1)]
df1 = df0_0.drop(columns=["26","27"])
df1_0 = df1[df1.apply(lambda row: (row['0'] == 30.0) and (True) and (True) and (True) and (True) and (True) or (row['0'] == 30.0) and (row['1'] == 43.0) and (row['2'] == -0.0013) and (row['3'] == -0.0002) and (row['4'] == 100.0) and (row['5'] == 518.67), axis=1)]
df2=df1_0.rename(columns={ "0":"id","1":"cycle","2":"setting1","3":"setting2","4":"setting3","5":"s1","6":"s2","7":"s3","8":"s4","9":"s5","10":"s6","11":"s7","12":"s8","13":"s9","14":"s10","15":"s11","16":"s12","17":"s13","18":"s14","19":"s15","20":"s16","21":"s17","22":"s18","23":"s19","24":"s20","25":"s21" })
df2_0 = df2[df2.apply(lambda row: (row['id'] == 30.0) and (True) and (True) and (True) and (True) and (True) or (row['id'] == 30.0) and (row['cycle'] == 43.0) and (row['setting1'] == -0.0013) and (row['setting2'] == -0.0002) and (row['setting3'] == 100.0) and (row['s1'] == 518.67), axis=1)]
df3 = df2_0.sort_values(by=["id","cycle"])
df3_0 = df3[df3.apply(lambda row: (row['id'] == 30.0) and (True) and (True) and (True) and (True) and (True), axis=1)]
df3_1 = df3[df3.apply(lambda row: (row['id'] == 30.0) and (row['cycle'] == 43.0) and (row['setting1'] == -0.0013) and (row['setting2'] == -0.0002) and (row['setting3'] == 100.0) and (row['s1'] == 518.67), axis=1)]
df4 = pickle.load(open("data_27.pickle",'rb'))
df4.columns = [str(c) for c in df4.columns]
df4["index"] = df4.index
df4 = df4[df4.apply(lambda row: None, axis=1)]
df4_0 = df4[df4.apply(lambda row: None, axis=1)]
df5 = df4_0.drop(columns=["26","27"])
df5_0 = df5[df5.apply(lambda row: None, axis=1)]
df6=df5_0.rename(columns={ "0":"id","1":"cycle","2":"setting1","3":"setting2","4":"setting3","5":"s1","6":"s2","7":"s3","8":"s4","9":"s5","10":"s6","11":"s7","12":"s8","13":"s9","14":"s10","15":"s11","16":"s12","17":"s13","18":"s14","19":"s15","20":"s16","21":"s17","22":"s18","23":"s19","24":"s20","25":"s21" })
df6_0 = df6[df6.apply(lambda row: None, axis=1)]
df7 = pickle.load(open("data_32.pickle",'rb'))
df7.columns = [str(c) for c in df7.columns]
df7["index"] = df7.index
df7 = df7[df7.apply(lambda row: None, axis=1)]
df7_0 = df7[df7.apply(lambda row: None, axis=1)]
df8 = df7_0.drop(columns=["1"])
df8_0 = df8[df8.apply(lambda row: None, axis=1)]
df9=df8_0.rename(columns={ "0":"more" })
df9_0 = df9[df9.apply(lambda row: None, axis=1)]
df10 = df3_0.groupby(["id"]).agg({ "cycle":"max" }).reset_index().rename(columns={ "cycle":"cycle" })
df10_0 = df10[df10.apply(lambda row: (row['id'] == 30.0) and (True) and (True) and (True) and (True) and (True), axis=1)]
df11=df3_1.rename(columns={ "cycle":"cycle_x" })
df11_0 = df11[df11.apply(lambda row: (row['id'] == 30.0) and (row['cycle'] == 43.0) and (row['setting1'] == -0.0013) and (row['setting2'] == -0.0002) and (row['setting3'] == 100.0) and (row['s1'] == 518.67), axis=1)]
df12=df10_0.rename(columns={ "cycle":"cycle_y" })
df12_0 = df12[df12.apply(lambda row: (row['id'] == 30.0) and (True) and (True) and (True) and (True) and (True), axis=1)]
df13 = df11_0.merge(df12_0, left_on = ["id"], right_on = ["id"])
df13_0 = df13[df13.apply(lambda row: (row['id'] == 30.0) and (row['cycle_x'] == 43.0) and (row['setting1'] == -0.0013) and (row['setting2'] == -0.0002) and (row['setting3'] == 100.0) and (row['s1'] == 518.67), axis=1)]
df14 = df13_0
df14["RUL"] = df14.apply(lambda xxx__: (xxx__['cycle_y']-xxx__['cycle_x']) , axis=1)
df14_0 = df14[df14.apply(lambda row: (row['id'] == 30.0) and (row['cycle_x'] == 43.0) and (row['setting1'] == -0.0013) and (row['setting2'] == -0.0002) and (row['setting3'] == 100.0) and (row['s1'] == 518.67), axis=1)]
df15 = df14_0.drop(columns=["cycle_y"])
df15_0 = df15[df15.apply(lambda row: (row['id'] == 30.0) and (row['cycle_x'] == 43.0) and (row['setting1'] == -0.0013) and (row['setting2'] == -0.0002) and (row['setting3'] == 100.0) and (row['s1'] == 518.67), axis=1)]
pickle.dump(df15, open('temp//result_after_pushdown.p', 'wb'))
