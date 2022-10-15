import pandas as pd
import numpy as np
import pickle
rng = np.random.RandomState(0)
df = pd.DataFrame({'key': ['A', 'B', 'C', 'A', 'B', 'C'],'data1': range(6),'data2': rng.randint(0, 10, 6)},columns = ['key', 'data1', 'data2'])
pickle.dump(df, open('df.pickle','wb'))
def norm_by_data2(x):
    x['data1'] /= x['data2'].sum()
    return x
out = df.groupby('key').apply(norm_by_data2)
