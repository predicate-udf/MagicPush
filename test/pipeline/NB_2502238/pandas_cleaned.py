import pandas as pd


tips = pd.read_csv('tips.csv')
tips['tip_pct'] = tips['tip'] / tips['total_bill']

def top(df, n=5, column='tip_pct'):
    return df.sort_values(by=column)[-n:]
tips.groupby('smoker', group_keys=False).apply(top)
