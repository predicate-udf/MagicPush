import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df1=pickle.load(open('data_1.pickle','rb'))
df2 = df0.merge(df1, how='left', left_on=["transaction_id"],right_on=["transaction_id"])
df4 = df2.rename(columns={ "app_name_x":"app_name","log_date_x":"log_date","test_name_x":"test_name","test_case_x":"test_case","user_id_x":"user_id","log_date_y":"click_date" })
df5 = df4[["transaction_id","log_date","click_date","test_case","user_id"]]
df5['是否被点击'] = df5.apply(lambda xxx__: 1 if not pd.isnull(xxx__['click_date']) else 0, axis=1)
df8 = df5[["transaction_id","log_date","test_case","user_id","是否被点击"]]
df9 = pd.pivot_table(df8, index='test_case',columns='是否被点击',values='user_id',aggfunc='count').reset_index()
print([{c:df9[c].dtype} for c in df9.columns])
df9["点击率"] = df9.apply(lambda xxx__: (xxx__[1] / (xxx__[1]+xxx__[0])), axis=1)
df9["点击率"] = df9.apply(lambda xxx__: format(xxx__['点击率'], '.2%'), axis=1)
