import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

df_clean = InitTable('twitter-archive-enhanced.csv')
df_clean1 = FillNA(df_clean, 'in_reply_to_status_id', 0)
df_clean2 = FillNA(df_clean1, 'in_reply_to_user_id', 0)
df_clean3 = FillNA(df_clean2, 'retweeted_status_id', 0)
df_clean4 = FillNA(df_clean3, 'retweeted_status_user_id', 0)
df_clean5 = FillNA(df_clean4, 'retweeted_status_timestamp',0)
df_clean6 = FillNA(df_clean5, 'expanded_urls', 0)

melt0 = DropColumns(df_clean6, ['source', 'retweeted_status_timestamp', 'timestamp', 'retweeted_status_user_id', 'text', 'retweeted_status_id', 'expanded_urls', 'in_reply_to_status_id', 'in_reply_to_user_id'])
# [doggo]=='puppo' or [floofer]=='puppo' or [pupper]=='puppo' or [puppo]=='puppo'
melt1 = UnPivot(melt0, ['tweet_id','rating_numerator','rating_denominator','name'], \
    ['doggo','floofer','pupper','puppo'], value_name='dog_stage')
melt2 = DropColumns(melt1, ['variable'])
melt3 = Filter(melt2, BinOp(Field('dog_stage'), '==', Constant('puppo')))


# input
#col, 'doggo','floofer','pupper','puppo'
#xx , 1,2,3,4
# xx, 2,1,3,4

#output
# col , dog_stage, default
# xx, 1, doggo
# xx, 2, floofer
# xx, 3, pupper
# xx, 4, puppo
# xx, 2, doggo
# xx, 1, floofer
# xx, 3, pupper
# xx, 4, puppo


# dog_stage == 1
# df['doggo']  == 1 or df['floofer']  == 1 or df['pupper']  == 1 or df['puppo']  == 1

# default == 'puppo'
# projection