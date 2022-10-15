import pandas as pd
import pickle
import numpy as np
df1=pickle.load(open('data_1.pickle','rb'))
df1["area"] = df1.apply(lambda xxx__: xxx__["Площадь, тыс.км2"], axis=1)
df1["area"] = df1.apply(lambda xxx__: xxx__["Площадь, тыс.км2"].replace(',','') if type(xxx__["Площадь, тыс.км2"]) is str else '0', axis=1)
df1["area"] = df1["area"].astype("float")
df1["density"] = df1.apply(lambda xxx__: (xxx__["Численность населения (2002  г.), чел"] / (1000*xxx__["area"]) if xxx__["area"]!=0 else 0 ), axis=1)

