import sys
# from ppl_interface import *
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")

import z3
import dis
from interface import *
from util import *
import random
# from constraint import *
from predicate import *
from generate_input_filters import *
from compare_pushdown_result import get_output_filter, check_pushdown_result, get_output_filter_all_operators
import os
from table_constraint import *


op0 = InitTable("Titanic_Dataset.pickle")
op1 = InitTable("Titanic_Dataset.pickle")
#op2 = CrossTableUDF(op0, op1, 'Age', \
#    lambda row: row['Age'], lambda row, x, y: x + y['Age'] if (pd.isnull(row['Age']) and row['Pclass']==y['Pclass']) else x)
# Path(pipeline: [], cond: ScalarCompute) 
# Path input can be df / row / group, schema, group_key..

sub_op0 = SubpipeInput(op0, 'table')
sub_op_row = SubpipeInput(op1, 'row')
filter_row =  ScalarComputation({'x':sub_op_row}, 'lambda x: x["Pclass"]')
sub_op1 = Filter(sub_op0, BinOp(Field('Pclass'), '==',filter_row))
sub_op_sum = AllAggregate(sub_op1, Value(0), 'lambda x,y: x+y["Age"]')
sub_op_count = AllAggregate(sub_op1, Value(0), 'lambda x,y: x+1')
sub_op2 = ScalarComputation({'s':sub_op_sum,'c':sub_op_count}, 'lambda s,c: s/c')
sub_op3 = ScalarComputation({'x':sub_op_row}, 'lambda x: x["Age"]')
cond1 = ScalarComputation({'x':sub_op_row}, 'lambda x: pd.isnull(x["Age"])')
cond2 = ScalarComputation({'x':sub_op_row}, 'lambda x: not pd.isnull(x["Age"])')
op2 = CrosstableUDF(op1, 'Age', \
     SubPipeline([PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, sub_op_sum, sub_op_count, sub_op2], cond1), \
         PipelinePath([sub_op_row, sub_op3], cond2)]))

# left [sub_op_row, filter_row]
# right []
 # output filter  + filter (right, left)
# table row(left)


# the subpipeinput should also 
ops = [op0, op1, op2]
#Or(cond1 And xx, cond2 And)
# print(sub_op_row in sub_op3.dependent_ops)
# print(get_previous_op([sub_op0, sub_op_row, filter_row, sub_op1, sub_op_sum, sub_op_count, sub_op2], sub_op_row))
# exit()

op3 = FillNA(op2, 'Embarked', 'S')
op4 = GetDummies(op3, ['Sex'], {'male':'int','female':'int'}) # (dependent_op, cols)
op5 = GetDummies(op3, ['Embarked'], {'C':'int','Q':'int','S':'int'})
op6 = GetDummies(op3, ['Pclass'],{1:'int',2:'int',3:'int'})
op7 = DropColumns(op3, ['PassengerId','Sex','Embarked','Name','Ticket','Pclass'])
op8 = ConcatColumn([op7, op4, op5, op6])


# ops = [op0, op1, op2, op3, op4, op5, op6, op7, op8]

# ops= [op0, op1, op2]
# for op in ops:
#     get_constraint(op)

#op8.constraints.append(FunctionalDependency('Price', ['Street'], 'lambda x: 2500000 if x["Street"] == \'NORTH SQUARE\' else 0'))
# op8.append(DomainConstraint({'Avg_Price', set(2500000)}))

# for o in op8.constraints:
#     #if isinstance(o, DomainConstraint):
#     print(o)
# exit()
output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


#output_filter = get_output_filter(ops, './temp')

#output_filter = And(BinOp(Field('Street'), '==', Constant('NORTH SQUARE')), BinOp(Field('Avg_Price'), '==', Constant(2500000)))
# output_filter = And(BinOp(Field('Sex'), '==', Constant('female')), BinOp(Field('Pclass'), '==', Constant(1)))
output_filter = And(BinOp(Field('Age'), '==', Constant('30')),And(BinOp(Field('Sex'), '==', Constant('female')), BinOp(Field('Pclass'), '==', Constant(1))))


# for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
#     if(op_i == ops[-1]):
#         output_filter_i = {None:output_filter}
#     else:
#         output_filter_i = generate_output_filter_from_previous(op_i, ops)
#     output_filter = AllOr(*list(output_filter_i.values()))
#     inference_i = op_i.get_inference_instance(output_filter)
#     inference_i.input_filters = generate_input_filters_general(op_i, inference_i, output_filter, output_schemas)
#     # print(output_filter_i)
#     print(op_id, ':')
#     print_input_filters(inference_i)
#     print(type(op_i))
#     assert(inference_i.check_small_model())
#     assert(inference_i.verify_correct())

for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = {None:output_filter}
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    output_filter = AllOr(*list(output_filter_i.values()))
    inference_i = op_i.get_inference_instance(output_filter)
    last_return = None
    i = 0
    while True:
        last_return, inference_i.input_filters = generate_input_filters_general(op_i, inference_i, output_filter, output_schemas, last_return)
        i += 1
        if (i >5):
            break
        # if inference_i.check_small_model() and inference_i.verify_correct():
        #     break
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    print(type(op_i))