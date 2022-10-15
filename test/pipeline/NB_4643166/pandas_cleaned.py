import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df2 = df0.drop(columns=["number"])
df3 = df2.set_index(keys="country")
df3['lngd'] = df3.apply(lambda xxx__: (xxx__['popgrowth']/100 + 0.05), axis=1)
df3['ls'] = df3.apply(lambda xxx__: (xxx__['i_y']/100), axis=1)
df3["ls_lngd"] = df3.apply(lambda xxx__: (xxx__["ls"] - xxx__["lngd"]), axis=1)
