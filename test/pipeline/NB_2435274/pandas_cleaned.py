import pandas as pd

insurance = pd.read_csv('insurance.csv')

def get_stats(group):
    return str(group['charges'].min()) + ',' + str(group['charges'].max())

out = insurance.groupby(['sex','smoker','region']).apply(get_stats).reset_index()
