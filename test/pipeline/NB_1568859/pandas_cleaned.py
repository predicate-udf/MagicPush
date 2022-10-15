import pandas as pd

df_clean = pd.read_csv('twitter-archive-enhanced.csv')
df_clean['in_reply_to_status_id']=df_clean['in_reply_to_status_id'].fillna(0)
df_clean['in_reply_to_user_id']=df_clean['in_reply_to_user_id'].fillna(0)
df_clean['retweeted_status_id']=df_clean['retweeted_status_id'].fillna(0)
df_clean['retweeted_status_user_id']=df_clean['retweeted_status_user_id'].fillna(0)
df_clean['retweeted_status_timestamp']=df_clean['retweeted_status_timestamp'].fillna(0)
df_clean['expanded_urls']=df_clean['expanded_urls'].fillna(0)

melt1 = df_clean[['tweet_id','rating_numerator','rating_denominator','name', 'doggo', 'floofer', 'pupper','puppo']].copy()
print(melt1['puppo'].value_counts())
melt1=melt1.melt(id_vars=['tweet_id','rating_numerator','rating_denominator','name'],value_name='dog_stage')
melt1= melt1.drop('variable', axis=1)
melt1 = melt1[(melt1.dog_stage =='puppo')]
