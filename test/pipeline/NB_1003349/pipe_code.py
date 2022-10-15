import pickle
# df0 = pickle.load(open("data_0.pickle",'rb'))
# df0.columns = [str(c) for c in df0.columns]
# df0["index"] = df0.index
# print(df0.columns)
# df0 = df0[(df0['knob_UNROLL_FACTOR1'] == 1.0) & (df0['knob_UNROLL_FACTOR2'] == 2.0) & (df0['knob_UNROLL_FACTOR3'] == 1.0) & (df0['knob_SUBDIM_X'] == 4.0) & (df0['knob_SUBDIM_Y'] == 1.0) & (df0['obj1_x'] == 11.953052)]
# df0_0 = df0[(df0['knob_UNROLL_FACTOR1'] == 1.0) & (df0['knob_UNROLL_FACTOR2'] == 2.0) & (df0['knob_UNROLL_FACTOR3'] == 1.0) & (df0['knob_SUBDIM_X'] == 4.0) & (df0['knob_SUBDIM_Y'] == 1.0) & (df0['obj1_x'] == 11.953052)]
# df1 = pickle.load(open("data_1.pickle",'rb'))
# df1.columns = [str(c) for c in df1.columns]
# df1["index"] = df1.index
# df1 = df1[(df1['knob_UNROLL_FACTOR1'] == 1.0) & (df1['knob_UNROLL_FACTOR2'] == 2.0) & (df1['knob_UNROLL_FACTOR3'] == 1.0) & (df1['knob_SUBDIM_X'] == 4.0) & (df1['knob_SUBDIM_Y'] == 1.0) & (df1['obj1_x'] == 11.953052)]
# df1_0 = df1[(df1['knob_UNROLL_FACTOR1'] == 1.0) & (df1['knob_UNROLL_FACTOR2'] == 2.0) & (df1['knob_UNROLL_FACTOR3'] == 1.0) & (df1['knob_SUBDIM_X'] == 4.0) & (df1['knob_SUBDIM_Y'] == 1.0) & (df1['obj1_x'] == 11.953052)]
# df2 = df0_0
# df2["mem_partition_size"] = df2.apply(lambda xxx__: xxx__['knob_DATA_BLOCK'], axis=1)
# df2_0 = df2[(df2['knob_UNROLL_FACTOR1'] == 1.0) & (df2['knob_UNROLL_FACTOR2'] == 2.0) & (df2['knob_UNROLL_FACTOR3'] == 1.0) & (df2['knob_SUBDIM_X'] == 4.0) & (df2['knob_SUBDIM_Y'] == 1.0) & (df2['obj1_x'] == 11.953052)]
# df3 = df1_0
# df3["mem_partition_size"] = df3.apply(lambda xxx__: xxx__['knob_DATA_BLOCK'], axis=1)
# df3_0 = df3[(df3['knob_UNROLL_FACTOR1'] == 1.0) & (df3['knob_UNROLL_FACTOR2'] == 2.0) & (df3['knob_UNROLL_FACTOR3'] == 1.0) & (df3['knob_SUBDIM_X'] == 4.0) & (df3['knob_SUBDIM_Y'] == 1.0) & (df3['obj1_x'] == 11.953052)]
# df4 = df3_0.drop(columns=["knob_I_B"])
# df4_0 = df4[(df4['knob_UNROLL_FACTOR1'] == 1.0) & (df4['knob_UNROLL_FACTOR2'] == 2.0) & (df4['knob_UNROLL_FACTOR3'] == 1.0) & (df4['knob_SUBDIM_X'] == 4.0) & (df4['knob_SUBDIM_Y'] == 1.0) & (df4['obj1_x'] == 11.953052)]
# df5 = df4_0.drop(columns=["knob_MAT_SIZE"])
# df5_0 = df5[(df5['knob_UNROLL_FACTOR1'] == 1.0) & (df5['knob_UNROLL_FACTOR2'] == 2.0) & (df5['knob_UNROLL_FACTOR3'] == 1.0) & (df5['knob_SUBDIM_X'] == 4.0) & (df5['knob_SUBDIM_Y'] == 1.0) & (df5['obj1_x'] == 11.953052)]
# df6 = df5_0.drop(columns=["knob_DATA_BLOCK"])
# df6_0 = df6[(df6['knob_UNROLL_FACTOR1'] == 1.0) & (df6['knob_UNROLL_FACTOR2'] == 2.0) & (df6['knob_UNROLL_FACTOR3'] == 1.0) & (df6['knob_SUBDIM_X'] == 4.0) & (df6['knob_SUBDIM_Y'] == 1.0) & (df6['obj1_x'] == 11.953052)]
# df7 = df2_0.drop(columns=["knob_I_B"])
# df7_0 = df7[(df7['knob_UNROLL_FACTOR1'] == 1.0) & (df7['knob_UNROLL_FACTOR2'] == 2.0) & (df7['knob_UNROLL_FACTOR3'] == 1.0) & (df7['knob_SUBDIM_X'] == 4.0) & (df7['knob_SUBDIM_Y'] == 1.0) & (df7['obj1_x'] == 11.953052)]
# df8 = df7_0.drop(columns=["knob_MAT_SIZE"])
# df8_0 = df8[(df8['knob_UNROLL_FACTOR1'] == 1.0) & (df8['knob_UNROLL_FACTOR2'] == 2.0) & (df8['knob_UNROLL_FACTOR3'] == 1.0) & (df8['knob_SUBDIM_X'] == 4.0) & (df8['knob_SUBDIM_Y'] == 1.0) & (df8['obj1_x'] == 11.953052)]
# df9 = df8_0.drop(columns=["knob_DATA_BLOCK"])
# df9_0 = df9[(df9['knob_UNROLL_FACTOR1'] == 1.0) & (df9['knob_UNROLL_FACTOR2'] == 2.0) & (df9['knob_UNROLL_FACTOR3'] == 1.0) & (df9['knob_SUBDIM_X'] == 4.0) & (df9['knob_SUBDIM_Y'] == 1.0) & (df9['obj1_x'] == 11.953052)]
# df10 = df6_0.sort_values(by=["index"])
# df10_0 = df10[(df10['knob_UNROLL_FACTOR1'] == 1.0) & (df10['knob_UNROLL_FACTOR2'] == 2.0) & (df10['knob_UNROLL_FACTOR3'] == 1.0) & (df10['knob_SUBDIM_X'] == 4.0) & (df10['knob_SUBDIM_Y'] == 1.0) & (df10['obj1_x'] == 11.953052)]
# df11 = df9_0.sort_values(by=["index"])
# df11_0 = df11[(df11['knob_UNROLL_FACTOR1'] == 1.0) & (df11['knob_UNROLL_FACTOR2'] == 2.0) & (df11['knob_UNROLL_FACTOR3'] == 1.0) & (df11['knob_SUBDIM_X'] == 4.0) & (df11['knob_SUBDIM_Y'] == 1.0) & (df11['obj1_x'] == 11.953052)]
# df12 = df10_0.merge(df11_0, left_on = ["knob_UNROLL_FACTOR1","knob_UNROLL_FACTOR2","knob_UNROLL_FACTOR3","mem_partition_size","knob_SUBDIM_X","knob_SUBDIM_Y"], right_on = ["knob_UNROLL_FACTOR1","knob_UNROLL_FACTOR2","knob_UNROLL_FACTOR3","mem_partition_size","knob_SUBDIM_X","knob_SUBDIM_Y"])
# df12_0 = df12[(df12['knob_UNROLL_FACTOR1'] == 1.0) & (df12['knob_UNROLL_FACTOR2'] == 2.0) & (df12['knob_UNROLL_FACTOR3'] == 1.0) & (df12['knob_SUBDIM_X'] == 4.0) & (df12['knob_SUBDIM_Y'] == 1.0) & (df12['obj1_x'] == 11.953052)]
# pickle.dump(df12, open('temp//result_after_pushdown.p', 'wb'))
df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df1 = pickle.load(open("data_1.pickle",'rb'))
df1.columns = [str(c) for c in df1.columns]
df1["index"] = df1.index
df2 = df0
df2["mem_partition_size"] = df2.apply(lambda xxx__: xxx__['knob_DATA_BLOCK'], axis=1)
df3 = df1
df3["mem_partition_size"] = df3.apply(lambda xxx__: xxx__['knob_DATA_BLOCK'], axis=1)
df4 = df3.drop(columns=["knob_I_B"])
df5 = df4.drop(columns=["knob_MAT_SIZE"])
df6 = df5.drop(columns=["knob_DATA_BLOCK"])
df7 = df2.drop(columns=["knob_I_B"])
df8 = df7.drop(columns=["knob_MAT_SIZE"])
df9 = df8.drop(columns=["knob_DATA_BLOCK"])
df10 = df6.sort_values(by=["index"])
df11 = df9.sort_values(by=["index"])
df12 = df10.merge(df11, left_on = ["knob_UNROLL_FACTOR1","knob_UNROLL_FACTOR2","knob_UNROLL_FACTOR3","mem_partition_size","knob_SUBDIM_X","knob_SUBDIM_Y"], right_on = ["knob_UNROLL_FACTOR1","knob_UNROLL_FACTOR2","knob_UNROLL_FACTOR3","mem_partition_size","knob_SUBDIM_X","knob_SUBDIM_Y"])

final_output = df12[df12.apply(lambda row: (((((row['knob_UNROLL_FACTOR1'] > 1.0) and (row['knob_UNROLL_FACTOR2'] < 1.0)) and (row['knob_UNROLL_FACTOR3'] > 2.0)) and (row['knob_SUBDIM_X'] == 16.0)) and (row['knob_SUBDIM_Y'] >= 16.0)) and (row['obj1_x'] != 7.59017), axis=1)]
print(final_output)
pickle.dump(final_output, open('temp//result_pred_on_output.p', 'wb'))