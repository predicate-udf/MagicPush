import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

df1 = InitTable('df_concatenated_station_v3.csv')

sub_op0 = SubpipeInput(df1, 'group', ['DAY'])
sub_op1 = TopN(sub_op0, 5, ['IMPRESSIONS'])
out = CogroupedMap(SubPipeline(PipelinePath([sub_op0, sub_op1])))

top50 = DropColumns(out, ['Entered', 'Exited'])
total_traffic = GroupBy(top50, ['DAY'], {'IMPRESSIONS':'sum'}, {'IMPRESSIONS':'IMPRESSIONS'})
top50_1 = InnerJoin(top50, total_traffic, ['DAY'], ['DAY'])
top50_2 = Rename(top50_1, {"IMPRESSIONS_x":"IMPRESSIONS_station","IMPRESSIONS_y":"IMPRESSIONS_total"})
top50_3 = Filter(top50_2, BinOp(Field("IMPRESSIONS_station"), ">", Constant(85007)))
top50_4 = SetItem(top50_3, 'density_score', "lambda top50: top50['IMPRESSIONS_station'] / top50['IMPRESSIONS_total']")
top50_5 = SetItem(top50_4, 'slots', "lambda val: round(val['density_score']) * 150")
top50_6 = Filter(top50_5, BinOp(Field('DAY'), '==', Constant(12)))
