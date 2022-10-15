import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *


op0 = InitTable("takehome_users.pickle")
op1 = InitTable("takehome_user_engagement.pickle")
op2 = ChangeType(op1, 'datetime', 'time_stamp', 'time_stamp')
op3 = GroupBy(op2, ['user_id'], {'visited':"rolling('7d').sum()"}, {'visited':'visited'})
op4 = Filter(op3, BinOp(Field('user_id'), '==', Constant(3)))

sub_op0 = SubpipeInput(op4)
sub_op_row = SubpipeInput(op0)
sub_op1 = Filter(sub_op0, BinOp(Field('user_id'), '==', ScalarComputation({'x':sub_op_row}, 'lambda x: x["object_id"]')))
sub_op2 = AllAggregate(sub_op1, Value(0), "lambda x,y: x+1")
cond1 = ScalarComputation({'count':sub_op2}, "lambda count: count>0")
value1 = ScalarComputation({'x':sub_op_row}, "1")
cond2 = ScalarComputation({'count':sub_op2}, "lambda count: count<=0")
value2 = ScalarComputation({'x':sub_op_row}, "0")
op5 = CrosstableUDF(op0, 'adopted', \
    SubPipeline([PipelinePath([sub_op0, sub_op_row, sub_op1, sub_op2, value1], cond1), \
        PipelinePath([sub_op0, sub_op_row, sub_op1, sub_op2, value2], cond2)]))

op6 = Copy(op5)
#op7 = CrossTableUDF(op5, op6, 'invited_adopte', \
#    lambda row: 0, lambda row,x,y: y['adopted'] if (~pd.isnull(row['invited_by_user_id']) and row['invited_by_user_id']==y['object_id']) else x)
sub_op0 = SubpipeInput(op6)
sub_op_row = SubpipeInput(op5)
sub_op1 = Filter(sub_op0, BinOp(Field('object_id'), '==', ScalarComputation({'x':sub_op_row}, 'lambda x: x["invited_by_user_id"]')))
sub_op2 = AllAggregate(sub_op1, Value(0, True), "lambda x,y: y['adopted']")
sub_op3 = ScalarComputation({'adopted_value':sub_op2},"lambda adopted_value: adopted_value+1")
cond1 = ScalarComputation({'row':sub_op_row}, 'lambda row: ~np.isnan(row)')
cond2 = ScalarComputation({'row':sub_op_row}, 'lambda row: np.isnan(row)')
sub_value2 = ScalarComputation({'x':sub_op_row}, 'lambda x: 0')
op7 = CrosstableUDF(op5, 'invited_adopted', \
    SubPipeline([PipelinePath([sub_op0, sub_op_row, sub_op1, sub_op2, sub_op3], cond1), \
        PipelinePath([sub_op_row, sub_value2], cond2)]))
