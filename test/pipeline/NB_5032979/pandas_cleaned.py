import pandas as pd

df = pd.read_csv('combined.csv')

df['text'] = df['state'].astype(str) + '\n' + 'Positive: ' + df['positive'].astype(str) + ', ' + 'Deaths: '+ df['death'].astype(str) + ', ' + 'Negative: '+ df['negative'].astype(str) + ', ' + 'Total Tested: '+ df['total'].astype(str)

df = df[df['state'] !=("GU")]

df = df[df['state'] !=("PR")]

df = df[df['state'] !=("AS")]

df = df[df['state'] !=("MP")]

df = df[df['state'] !=("PRI")]

df = df[df['state'] !=("VIR")]

df = df[df['state'] !=("VI")]

# print(df['state'].drop_duplicates().shape)
# print(df[['state',"Emergency Declaration Date"]].drop_duplicates().shape)
# print(df[['state',"Stay At Home Order Date"]].drop_duplicates().shape)
# exit(0)

def calculate_num_claims_since_emergency_declaration(x: pd.DataFrame) -> int:    
    declaration_date = x["Emergency Declaration Date"].values[0]
    return x[x.date >= declaration_date].number_of_iclaims.sum()

iclaims_since_emergency_by_state = df.groupby("state").apply(calculate_num_claims_since_emergency_declaration)
df['iclaims_since_emergency'] = df.state.apply(lambda x: iclaims_since_emergency_by_state[x] if x in iclaims_since_emergency_by_state else None)

def calculate_num_claims_since_stayathome_order(x: pd.DataFrame) -> int:    
    stayathome_date = x["Stay At Home Order Date"].values[0]
    return x[x.date >= stayathome_date].number_of_iclaims.sum()

iclaims_since_stayathome_by_state = df.groupby("state").apply(calculate_num_claims_since_stayathome_order)
df['iclaims_since_stayathome'] = df.state.apply(lambda x: iclaims_since_stayathome_by_state[x] if x in iclaims_since_stayathome_by_state else None)

df = df.sort_values(by=['date'])

df = df[df["date"]>="2020-01-30"]

df = df[df["death"].isna()==False]

df = df[df["positive"].isna()==False]

df['pct_death_over_positive'] = 100*(df['death']/df['positive'])

df['death_rate'] = df['death']/df['positive']

df['high_pct_death'] = df['pct_death_over_positive'].apply(lambda x: '1%+ death rate' if x >= 1 else "below 1% death rate")

df['pct_positive_out_of_total_tested'] = 100*(df['positive']/df['total'])

df['positive_over_pop'] = (df['positive']/df['POPESTIMATE2019'])*100

df['positive_per_100k'] = (df['positive_over_pop']/100)*100000

df['pct_pop_tested'] = (df['total']/df['POPESTIMATE2019'])*100
