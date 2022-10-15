import pickle
df0 = pickle.load(open("data_1.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df0 = df0[df0.apply(lambda row: if pd.isnull(row[employment_status]):0 row[employment_status]) > 0 and ( fill_value if pd.isnull(row[col]) else row[col])
) == 43 and ( if pd.isnull(row[age]): 0 row[age]) == 'Female' and ( if pd.isnull(row[gender]):    0row[gender]) == 'WhiteOnly' and ( if pd.isnull(row[race]):    0row[race]) == 'Non-Hispanic' and ( if pd.isnull(row[ethnicity]):    0row[ethnicity]) == 'Working' and ( if pd.isnull(row[employment_status]):    0row[employment_status]) == 40.0, axis=1)]
df0_0 = df0[df0.apply(lambda row: ( if pd.isnull(row[employment_status]):    0row[employment_status]) > 0 and ( fill_value if pd.isnull(row[col]) else row[col]
) == 43 and ( if pd.isnull(row[age]):    0row[age]) == 'Female' and ( if pd.isnull(row[gender]):    0row[gender]) == 'WhiteOnly' and ( if pd.isnull(row[race]):    0row[race]) == 'Non-Hispanic' and ( if pd.isnull(row[ethnicity]):    0row[ethnicity]) == 'Working' and ( if pd.isnull(row[employment_status]):    0row[employment_status]) == 40.0, axis=1)]
df1=df0_0
df1["age"] = df1["age"].fillna(value=0)
df1_0 = df1[df1.apply(lambda row: ( if pd.isnull(row[employment_status]):    0row[employment_status]) > 0 and (row['age'] == 43) and ( fill_value if pd.isnull(row[col]) else row[col]
) == 'Female' and ( if pd.isnull(row[gender]):    0row[gender]) == 'WhiteOnly' and ( if pd.isnull(row[race]):    0row[race]) == 'Non-Hispanic' and ( if pd.isnull(row[ethnicity]):    0row[ethnicity]) == 'Working' and ( if pd.isnull(row[employment_status]):    0row[employment_status]) == 40.0, axis=1)]
df2=df1_0
df2["gender"] = df2["gender"].fillna(value=0)
df2_0 = df2[df2.apply(lambda row: ( if pd.isnull(row[employment_status]):    0row[employment_status]) > 0 and (row['age'] == 43) and (row['gender'] == 'Female') and ( fill_value if pd.isnull(row[col]) else row[col]
) == 'WhiteOnly' and ( if pd.isnull(row[race]):    0row[race]) == 'Non-Hispanic' and ( if pd.isnull(row[ethnicity]):    0row[ethnicity]) == 'Working' and ( if pd.isnull(row[employment_status]):    0row[employment_status]) == 40.0, axis=1)]
df3=df2_0
df3["race"] = df3["race"].fillna(value=0)
df3_0 = df3[df3.apply(lambda row: ( if pd.isnull(row[employment_status]):    0row[employment_status]) > 0 and (row['age'] == 43) and (row['gender'] == 'Female') and (row['race'] == 'WhiteOnly') and ( fill_value if pd.isnull(row[col]) else row[col]
) == 'Non-Hispanic' and ( if pd.isnull(row[ethnicity]):    0row[ethnicity]) == 'Working' and ( if pd.isnull(row[employment_status]):    0row[employment_status]) == 40.0, axis=1)]
df4=df3_0
df4["ethnicity"] = df4["ethnicity"].fillna(value=0)
df4_0 = df4[df4.apply(lambda row: ( if pd.isnull(row[employment_status]):    0row[employment_status]) > 0 and (row['age'] == 43) and (row['gender'] == 'Female') and (row['race'] == 'WhiteOnly') and (row['ethnicity'] == 'Non-Hispanic') and ( fill_value if pd.isnull(row[col]) else row[col]
) == 'Working' and ( if pd.isnull(row[employment_status]):    0row[employment_status]) == 40.0, axis=1)]
df5=df4_0
df5["employment_status"] = df5["employment_status"].fillna(value=0)
df5_0 = df5[df5.apply(lambda row: ( fill_value if pd.isnull(row[col]) else row[col]
) > 0 and (row['age'] == 43) and (row['gender'] == 'Female') and (row['race'] == 'WhiteOnly') and (row['ethnicity'] == 'Non-Hispanic') and (row['employment_status'] == 'Working') and ( fill_value if pd.isnull(row[col]) else row[col]
) == 40.0, axis=1)]
df6=df5_0
df6["hours_worked_per_week"] = df6["hours_worked_per_week"].fillna(value=0)
df6_0 = df6[df6.apply(lambda row: (row['hours_worked_per_week'] > 0) and (row['age'] == 43) and (row['gender'] == 'Female') and (row['race'] == 'WhiteOnly') and (row['ethnicity'] == 'Non-Hispanic') and (row['employment_status'] == 'Working') and (row['hours_worked_per_week'] == 40.0), axis=1)]
df7=df6_0
df7["earnings_per_week"] = df7["earnings_per_week"].fillna(value=0)
df7_0 = df7[df7.apply(lambda row: (row['hours_worked_per_week'] > 0) and (row['age'] == 43) and (row['gender'] == 'Female') and (row['race'] == 'WhiteOnly') and (row['ethnicity'] == 'Non-Hispanic') and (row['employment_status'] == 'Working') and (row['hours_worked_per_week'] == 40.0), axis=1)]
df8 = df7_0[df7_0.apply(lambda row: row['hours_worked_per_week'] > 0, axis=1)]
df8_0 = df8[df8.apply(lambda row: (row['age'] == 43) and (row['gender'] == 'Female') and (row['race'] == 'WhiteOnly') and (row['ethnicity'] == 'Non-Hispanic') and (row['employment_status'] == 'Working') and (row['hours_worked_per_week'] == 40.0), axis=1)]
df9 = df8_0.copy()
df9_0 = df9[df9.apply(lambda row: (row['age'] == 43) and (row['gender'] == 'Female') and (row['race'] == 'WhiteOnly') and (row['ethnicity'] == 'Non-Hispanic') and (row['employment_status'] == 'Working') and (row['hours_worked_per_week'] == 40.0), axis=1)]
df10 = df9_0
df10["female"] = df10.apply(lambda xxx__: ({ "Female":1,"Male":0 }[xxx__["gender"]] if xxx__["gender"] in ["Female","Male"] else xxx__["gender"]), axis=1)
df10_0 = df10[df10.apply(lambda row: (row['age'] == 43) and (row['gender'] == 'Female') and (row['race'] == 'WhiteOnly') and (row['ethnicity'] == 'Non-Hispanic') and (row['employment_status'] == 'Working') and (row['hours_worked_per_week'] == 40.0), axis=1)]
df11 = df10_0
df11["person_hard_worker"] = df11.apply(lambda xxx__: 1 if xxx__['hours_worked_per_week'] > 38.8 else 0, axis=1)
df11_0 = df11[df11.apply(lambda row: (row['age'] == 43) and (row['gender'] == 'Female') and (row['race'] == 'WhiteOnly') and (row['ethnicity'] == 'Non-Hispanic') and (row['employment_status'] == 'Working') and (row['hours_worked_per_week'] == 40.0), axis=1)]
pickle.dump(df11, open('temp//result_after_pushdown.p', 'wb'))