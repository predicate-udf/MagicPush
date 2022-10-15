import pandas as pd
import pickle

cdNow = pd.read_csv('CDNOW.txt',sep = '\s+',header = None, names=['user','date','quantity','price'])
cdNow.date = pd.to_datetime(cdNow.date, format='%Y%m%d')
#pickle.dump(cdNow, open("CDNOW.pickle",'wb'))

cdNow['month'] = cdNow.date.astype('datetime64[M]')

cdNow.set_index('date', drop=True, inplace = True)

#cdNow.reset_index(drop=False, inplace=True)
def lifeCycle(x):
    return x.max() - x.min()

out = cdNow[['user','month']].groupby('user').apply(lifeCycle).groupby('month').agg('count').reset_index()
# 用内置函数效率好像更高

# df = cdNow[['user','month']].groupby('user').agg(['max','min'])

# print((df.month['max'] - df.month['min']).value_counts())
# def rebuyPeriod(x):
#     if len(x.date)>1:
#         return x.date.iloc[1] - x.date.iloc[0]
#     else:
#         return None
# cdNow[['user','date']].groupby('user').apply(rebuyPeriod).value_counts()
# def avgPeriod(x):
#     if len(x)>1:
#         return (x.iloc[-1] - x.iloc[0])/(len(x)-1)
#     else:
#         return None

# cdNow[['user','date']].groupby('user').apply(avgPeriod).date.value_counts()
# # 用pivot table
# pivoted_amount = cdNow.pivot_table(index='user',columns = 'month', values='quantity', aggfunc='count').fillna(0)
# columns_month = cdNow.month.sort_values().astype('str').unique()
# pivoted_amount.columns = columns_month
# pivoted_amount.head()
