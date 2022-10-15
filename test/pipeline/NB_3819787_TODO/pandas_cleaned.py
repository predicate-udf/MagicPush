import pandas as pd

nba = pd.read_csv("nba_draft_picks_final.csv")
df = pd.read_csv("ncaa_stats.csv")
df_new = df.groupby(['Player'])['Season'].transform(max) == df['Season']
stats = df[df_new]
#limit to the few columns we need from the NBA dataset
nba = nba[['Player','Rd','Pk','Year']]
#merge nba players and get their ncaa stats
draft_stats = pd.merge(nba,stats,on='Player',how='left')
draft_stats = draft_stats.dropna(subset=['Class']) 
#limit to the most recent NCAA season and drop the raw MP column (we only want Min/Gm)
draft_stats_test = draft_stats[draft_stats['Year'] < 2017]
test = stats[stats['Season'] == '2016-17']
draft_stats_vars = draft_stats_test.drop(['Rd','Pk','Year','Rk','Class','Season','Pos','School','Conf','G','MP'],axis=1)
test_16_vars = test.drop(['Class','Rk','Season','Pos','School','Conf','G'],axis=1)
print ("test df shape:",test_16_vars.shape)
print ("draft df shape:",draft_stats_vars.shape)
# concat the two DFs together into one big DF 
final = pd.concat([test_16_vars,draft_stats_vars])
print(final)