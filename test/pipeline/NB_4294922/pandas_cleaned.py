import pandas as pd
boxscores = pd.read_csv('boxscores.csv')

#boxscores = boxscores[boxscores['wonMatchup']==True]
positional_stats = (boxscores[(boxscores['slotId'] != 20)]
 .filter(items=['teamName', 'matchupPeriodId', 'position', 'appliedStatTotal', 'wonMatchup'], axis=1)
 # group by team, matchup, and postion and take the mean positional score using .agg:
 .groupby(['teamName', 'matchupPeriodId', 'position'])
 .agg({'appliedStatTotal': 'mean'})
 # Pivot table on 'position' to create new columns:
 .unstack('position')
 .reset_index())
# Create 'Won' column by taking the min of 'wonMatchup':
positional_stats['Won'] = boxscores.groupby(['teamName', 'matchupPeriodId']).agg({'wonMatchup': 'min'}).reset_index(drop=True)
# Rearrange columns:
positional_stats.columns = ['Team', 'Matchup', 'D/ST', 'QB', 'RB', 'TE', 'WR', 'Won']
df_team = positional_stats[positional_stats['Team'] == 'Team 4']
# Box scores for selected team in wins:
df_team_win = df_team[df_team['Won'] == True]
