import pandas as pd

real_wage = pd.read_csv('real-wage.csv')
select = ((real_wage.Series =='In 2018 constant prices at 2018 USD PPPs' ))
real_wage = real_wage[select][['Country','Time','Value']].reset_index(drop=True)
def normalize(group):
    x = group[group.Time==2007].Value.values[0]
    df = group.copy()
    df['Value'] = df.Value/x
    return df[['Time','Value']]
wage_index = real_wage.groupby('Country').apply(normalize).reset_index().drop('level_1',axis=1)
print(wage_index.head())
print(wage_index.shape)
country_select = ['Greece','United Kingdom','United States','France','Germany','Poland']
data = wage_index[(wage_index.Country in country_select) &(wage_index.Time>=2007)]
#China ， Time ==2000 or 2007 (value)