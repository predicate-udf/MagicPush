import pandas as pd
df = pd.read_csv('df_concatenated_station_v3.csv') #import later

def get_top_50(grp):
    return grp.sort_values("IMPRESSIONS", ascending = False).head(50)

top50 = df.groupby('DAY')[["IMPRESSIONS", "STATION"]].apply(get_top_50).reset_index().drop(['level_1'], axis=1)
total_traffic = top50.groupby(['DAY'])['IMPRESSIONS'].sum().reset_index()
top50 = top50.merge(total_traffic, on = 'DAY', suffixes = ['_station', '_total'])
top50 = top50[top50["IMPRESSIONS_station"]>85007]
top50['density_score'] = top50['IMPRESSIONS_station'] / top50['IMPRESSIONS_total']
top50['slots'] = top50['density_score'].map(lambda val: round(val * 150))
top50 = top50[top50.DAY==12]
print(top50.shape)