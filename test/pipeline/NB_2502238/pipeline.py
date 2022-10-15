import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

tips = InitTable('tips.csv')
tips1 = SetItem(tips, 'tip_pct', 'lambda x: x["tip"]/x["total_bill"]')

sub_op0 = SubpipeInput(tips1, 'group', ['smoker'])
sub_op1 = TopN(sub_op0, 5, ['tip_pct'])
out = CogroupedMap(SubPipeline(PipelinePath([sub_op0, sub_op1])))

