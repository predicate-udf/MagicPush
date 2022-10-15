from email.headerregistry import Group
import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

boxscores = InitTable('boxscores.csv')

df1 = Filter(boxscores, BinOp(Field('slotId'),'!=', Constant(20)))
df2 = DropColumns(df1, ['slotId', 'bye', 'W/L', 'playerName'])
df3 = GroupBy(df2, ['teamName', 'matchupPeriodId', 'position'], {'appliedStatTotal': 'mean'}, {'appliedStatTotal': 'appliedStatTotal'})
positional_stats = Pivot(df3, ['teamName', 'matchupPeriodId'], 'position', 'appliedStatTotal', None, {'D/ST':'float','QB':'float','RB':'float','TE':'float','WR':'float'})

sub_op0 = SubpipeInput(boxscores, 'table')
sub_op_row = SubpipeInput(positional_stats, 'row')
teamName = ScalarComputation({'row':sub_op_row}, 'lambda row: row["teamName"]')
matchupPeriodId = ScalarComputation({'row':sub_op_row}, 'lambda row: row["matchupPeriodId"]')
sub_op1 = Filter(sub_op0, And(BinOp(Field('teamName'), '==', teamName), BinOp(Field('matchupPeriodId'), '==', matchupPeriodId)))
sub_op2 = AllAggregate(sub_op1, Value(True), 'lambda x,y: False if not y["wonMatchup"]==True else x') # min of T/F on wonMatchup
positional_stats1 = CrosstableUDF(positional_stats, 'Won', SubPipeline(PipelinePath([sub_op0, sub_op_row, teamName, matchupPeriodId, sub_op1, sub_op2])))

positional_stats2 = Rename(positional_stats1, {'teamName':'Team','matchupPeriodId':'Matchup','QB':'QB', 'RB':'RB', 'TE':'TE', 'WR':'WR', 'Won':'Won'})
df_team = Filter(positional_stats2, BinOp(Field('Team'), '==', Constant('Team 4')))
df_team_win = Filter(df_team, BinOp(Field('Won'),'==',Constant(True)))
