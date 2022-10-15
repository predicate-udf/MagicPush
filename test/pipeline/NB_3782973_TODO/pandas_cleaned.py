
import pandas as pd
from preprocessing import preprocess
dfs = []

dfs.append(preprocess('../data/raw/CA/CA_1981-1985.txt'))
dfs.append(preprocess('../data/raw/CA/CA_1985-1989.txt'))
dfs.append(preprocess('../data/raw/CA/CA_1989-1993.txt'))
dfs.append(preprocess('../data/raw/CA/CA_1993-1997.txt'))

dfs.append(preprocess('../data/raw/CA/CA_1997-2001.txt'))

dfs.append(preprocess('../data/raw/CA/CA_2001-2005.txt'))

dfs.append(preprocess('../data/raw/CA/CA_2005-2009.txt'))

dfs.append(preprocess('../data/raw/CA/CA_2009-2015.txt'))
df = pd.concat(dfs)
df = df.dropna(axis=0)
df[['STN','YEARMODA','TEMP','DEWP','VISIB','WDSP','GUST','MAX','MIN','PRCP']].head(10)
df_subsetted = pd.read_csv('/datadrive/cong/replay/../datafiles/CA_1981-2015_subsetted2.csv', parse_dates=['YEARMODA'])
df_monthly = df_subsetted.groupby('STN').apply(lambda x: x.set_index('YEARMODA')[['PRCP', 'MXSPD', 'WDSP']].resample('M'))
df_monthly = df_monthly.reset_index()
def extract_novembers(df):
    df_timed = df.set_index('YEARMODA')[['PRCP', 'MXSPD', 'WDSP']]
    return df_timed[df_timed.index.month == 11]
df_novembers = df_monthly.groupby('STN').apply(extract_novembers)
df_novembers = df_novembers.reset_index()
dff = df_novembers.set_index('YEARMODA')['1990':'2015']
dff = dff.reset_index()
all_data = dff.pivot_table(index=['YEARMODA'], columns=['STN']).dropna(axis=1)