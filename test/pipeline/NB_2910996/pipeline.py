import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

df = InitTable('df.pickle')
sub_op0 = SubpipeInput(df, 'group', ['key'])
sub_op1 = AllAggregate(sub_op0, Value(0), "lambda x, y: x + y['data2']")
sub_op2 = SetItemWithDependency(sub_op0, 'data1', {'xsum':sub_op1,'x':sub_op0}, "lambda x,xsum: x['data1']/xsum")
out = CogroupedMap(SubPipeline(PipelinePath([sub_op0, sub_op1, sub_op2])))
