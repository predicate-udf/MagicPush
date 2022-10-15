import pandas as pd
import pickle

segments = pd.read_csv("transit_segments.csv", parse_dates=['st_time', 'end_time'])
#pickle.dump(segments, open('transit_segments.pickle', 'wb'))
vessels = pd.read_csv("vessel_information.csv", index_col='mmsi')
#pickle.dump(vessels, open("vessel_information.pickle", 'wb'))
def top(df, column, n=5):
    return df.sort_values(by=column, ascending=False)[:n]
segments_merged = pd.merge(vessels, segments, left_index=True, right_on='mmsi')
print(segments_merged.columns)
top3segments = segments_merged.groupby('mmsi').apply(top, column='seg_length', n=3)[['names', 'seg_length']]


# TOP : pushdown correct?
# groupby map
# for each group, as long as the pushdown is correct, in general correct
# topN pushdown
