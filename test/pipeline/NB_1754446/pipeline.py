import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

cdNow = InitTable('CDNOW.pickle')
cdNow1 = ChangeType(cdNow, "datetime", 'date','month')
cdNow2 = Rename(cdNow1, {'date':'index'})
cdNow3 = DropColumns(cdNow2, ['index','quantity', 'price'])

sub_op1 = SubpipeInput(cdNow3, 'group', ['user'])
sub_op2 = AllAggregate(sub_op1, Value(0, True), 'lambda x, y: y["month"] if y["month"] > x else x')
sub_op3 = AllAggregate(sub_op1, Value(0, True), 'lambda x, y: y["month"] if y["month"] < x else x')
sub_op4 = ScalarComputation({'max_':sub_op2, 'min_':sub_op3}, 'lambda max_, min_: max_-min_')
cdNow4 = SubPipeline(PipelinePath([sub_op1, sub_op2, sub_op3, sub_op4]))

cdNow5 = GroupBy(cdNow4, ['month'], {'user':'count'}, {'user':'user'})
