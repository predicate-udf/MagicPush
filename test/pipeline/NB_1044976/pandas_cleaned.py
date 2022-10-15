import pandas as pd
import pickle

df_gdppp = pd.read_excel('GDP_per_capita.xlsx', sheet_name='Data')
#pickle.dump(df_gdppp, open('GDP_per_capita.pickle','wb'))
df_groups = pd.read_csv('country_groups.csv')
#pickle.dump(df_groups, open('country_groups.pickle','wb'))
print(df_groups['CountryCode'].value_counts())
oecd_countries = df_groups.loc[df_groups.GroupCode == 'OED'].CountryCode.values
print(oecd_countries)
print(df_gdppp.columns)
df_oecd_wide = df_gdppp.loc[df_gdppp['Country Code'].isin(oecd_countries)]

print(df_oecd_wide.columns)
df_oecd = df_oecd_wide.melt(id_vars=['Country Name', 'Country Code'], var_name='year', value_name='GDPPC')
df_oecd.year = df_oecd.year.astype(int)
df_oecd = df_oecd.loc[df_oecd.year >= 1990]

def normalize_to_2000(df):
    ref = df.loc[df.year == 2000].iloc[0]['GDPPC']
    df.GDPPC /= ref  
    df.GDPPC = (df.GDPPC * 100) - 100
    return df
df_oecd_normalized = df_oecd.groupby('Country Name').apply(normalize_to_2000)
df_oecd_normalized = df_oecd_normalized.loc[df_oecd_normalized.year >= 2000]
df_oecd_normalized = df_oecd_normalized.loc[df_oecd_normalized['Country Code'=='ISR']]