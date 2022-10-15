
import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *
from predicate import *
from generate_input_filters import *

import numpy as np
import random
import pickle


op0 = InitTable('titanic.csv')

# titanic.groupby('sex').apply(c_deck_survival)
# def c_deck_survival(df_in):
#     c_passengers = df_in['cabin'].str.startswith('C').fillna(False)
#     return df_in.loc[c_passengers, 'survived'].mean()

sub_op0 = SubpipeInput(op0, 'group', ['sex'])
sub_op1 = Filter(sub_op0, Expr('lambda x: str(x["cabin"]).startswith("C")'))
sub_op2 = AllAggregate(sub_op1, Value(0), "lambda x,y: x+y['survived']") # sum
sub_op3 = AllAggregate(sub_op1, Value(0), "lambda x,y: x+1") # count
sub_op4 = ScalarComputation({'s':sub_op2,'c':sub_op3}, 'lambda s, c: s/c')
output = CogroupedMap(SubPipeline(PipelinePath([sub_op0, sub_op1, sub_op2, sub_op3, sub_op4])))


ops = [op0,  output]
output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = BinOp(Field('sex'), '==', Constant('female'))

print(output_filter)

for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = {None:output_filter}
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    output_filter = AllOr(*list(output_filter_i.values()))
    inference_i = op_i.get_inference_instance(output_filter)


    # inference_i.input_filters = generate_input_filters(op_i, inference_i, output_filter)

    # print(op_id, ':')
    # print_input_filters(inference_i)
    # print(type(op_i))
    # assert(inference_i.check_small_model())
    # assert(inference_i.verify_correct())
