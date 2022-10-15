import pandas as pd
import pickle

df = pickle.load(open("NB_4562158//data_65.pickle",'rb'))
df.columns = [str(c) for c in df.columns]
df["index"] = df.index

scale_factor = int(1000*1000*1000/sum(df.memory_usage(index=False).tolist()))
scale_df = pd.concat([df]*scale_factor)
print('scale factor = {} / {}'.format(scale_factor, scale_df.shape[0]))
pickle.dump(scale_df, open('/datadrive/yin/pipeline_opt_experiment/NB_4562158/data/scaled_data_65.pickle', 'wb'))
