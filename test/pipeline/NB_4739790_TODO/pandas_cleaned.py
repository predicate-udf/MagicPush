import pandas as pd
import numpy as np
import pickle

users = pd.read_csv("takehome_users.csv",encoding="ISO-8859-1")
user_engagement = pd.read_csv("takehome_user_engagement.csv",encoding="ISO-8859-1")
user_engagement['time_stamp'] = pd.to_datetime(user_engagement['time_stamp'])
user_engagement = user_engagement.set_index(keys='time_stamp')
check = user_engagement.groupby(by='user_id')['visited'].rolling('7d').sum() # TODO: aggr not implemented
adopted_user = check[check==3].index.unique()
def set_adopted(row):
    if row['object_id'] in adopted_user:
        return 1
    else:
        return 0
users['adopted'] = users.apply(set_adopted, axis=1)
def checkInvitedUser(row):
    if ~np.isnan(row):
        adopted_value = users.loc[users['object_id'] == row,'adopted'].values[0]
        return (adopted_value + 1)
    else:
        return 0
users['invited_adopted'] = users['invited_by_user_id'].apply(checkInvitedUser)