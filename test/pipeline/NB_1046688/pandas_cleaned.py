import pandas as pd
import pickle

billboard_raw_df = pd.read_csv('billboard.csv',encoding='latin1')
#pickle.dump(billboard_raw_df, open('billboard.pickle','wb'))

id_vars = billboard_raw_df.columns[:7]
# id_vars = ['year', 'artist.inverted', 'track', 'time', 'genre', 'date.entered', 'date.peaked']

top_tracks = ['(Hot S**t) Country Grammar', 'Absolutely (Story Of A Girl)', 'Amazed',\
       'Back Here', 'Bent', 'Breathe', 'Bring It All To Me',\
       'Case Of The Ex (Whatcha Gonna Do)', 'Everything You Want',\
       'Get It On.. Tonite', 'He Loves U Not', 'He Wasn\'t Man Enough',\
       'Higher', 'I Knew I Loved You', 'I Try', 'I Wanna Know',\
       'Independent Women Part I', 'Jumpin\' Jumpin\'', 'Kryptonite',\
       'Meet Virginia', 'Most Girls', 'My Love Is Your Love', 'Say My Name',\
       'Sexual (Li Da Di)', 'That\'s The Way It Is', 'There U Go', 'Thong Song',\
       'Try Again', 'With Arms Wide Open', 'You Sang To Me']

billboard_melted_df = pd.melt(billboard_raw_df,id_vars=id_vars)
billboard_melted_df.rename(columns={'variable':'week','value':'rank'},inplace=True)

billboard_melted_df['week'] = billboard_melted_df['week'].str.extract(r'x(\d+)')

billboard_melted_df['time'] = pd.to_datetime(billboard_melted_df['time']).apply(lambda x:x.hour*60 + x.minute)

billboard_melted_df['week'] = billboard_melted_df['week'].apply(int)

#ax = billboard_melted_df.query('track == "Music"').plot(x='week',y='rank',lw=3)

billboard_track_week_pivoted = billboard_melted_df.pivot_table(index='week',columns='track',values='rank')

print({"{}".format(x):'int' for x in list(billboard_track_week_pivoted.columns)})
ax = billboard_track_week_pivoted[top_tracks]
print(ax)


# input table
# week, track, rank

# output
# ['week', '(Hot S**t) Country Grammar', 'Absolutely (Story Of A Girl)', 'Amazed']

# track in [top_tracks]

# schame change in the pivot table
