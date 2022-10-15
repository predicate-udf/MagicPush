import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df1=pickle.load(open('data_1.pickle','rb'))
df0['mem_partition_size'] = df0.apply(lambda xxx__: xxx__['knob_DATA_BLOCK'], axis=1)
df1['mem_partition_size'] = df1.apply(lambda xxx__: xxx__['knob_DATA_BLOCK'], axis=1)
df29 = df1.drop(columns=["knob_I_B"])
df30 = df29.drop(columns=["knob_MAT_SIZE"])
df31 = df30.drop(columns=["knob_DATA_BLOCK"])
df32 = df0.drop(columns=["knob_I_B"])
df33 = df32.drop(columns=["knob_MAT_SIZE"])
df34 = df33.drop(columns=["knob_DATA_BLOCK"])
df47 = df31.reset_index()
df48 = df34.reset_index()
df49 = df47.merge(df48, left_on=["knob_UNROLL_FACTOR1","knob_UNROLL_FACTOR2","knob_UNROLL_FACTOR3","mem_partition_size","knob_SUBDIM_X","knob_SUBDIM_Y"],right_on=["knob_UNROLL_FACTOR1","knob_UNROLL_FACTOR2","knob_UNROLL_FACTOR3","mem_partition_size","knob_SUBDIM_X","knob_SUBDIM_Y"])

