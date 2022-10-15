import sys
# from ppl_interface import *
sys.path.append("../../../")
sys.path.append("../../")
import z3
import dis
from interface import *
from util import *
import random
from predicate import *
from generate_input_filters import *
from compare_pushdown_result import get_output_filter, check_pushdown_result
import os

op0 = InitTable("data_0.pickle")
op1 = InitTable("data_1.pickle")
op3 = SetItem(op0, 'mem_partition_size', 'lambda xxx__: xxx__[\'knob_DATA_BLOCK\']')
op4 = SetItem(op1, 'mem_partition_size', 'lambda xxx__: xxx__[\'knob_DATA_BLOCK\']')
op17 = DropColumns(op4, ["knob_I_B"])
op18 = DropColumns(op17, ["knob_MAT_SIZE"])
op19 = DropColumns(op18, ["knob_DATA_BLOCK"])
op20 = DropColumns(op3, ["knob_I_B"])
op21 = DropColumns(op20, ["knob_MAT_SIZE"])
op22 = DropColumns(op21, ["knob_DATA_BLOCK"])
op27 = SortValues(op19, ["index"])
op28 = SortValues(op22, ["index"])
op29 = InnerJoin(op27, op28, ["knob_UNROLL_FACTOR1","knob_UNROLL_FACTOR2","knob_UNROLL_FACTOR3","mem_partition_size","knob_SUBDIM_X","knob_SUBDIM_Y"],["knob_UNROLL_FACTOR1","knob_UNROLL_FACTOR2","knob_UNROLL_FACTOR3","mem_partition_size","knob_SUBDIM_X","knob_SUBDIM_Y"])

ops = [op0, op1, op3, op4, op17, op18, op19, op20, op21, op22, op27, op28, op29]

output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = get_output_filter(ops, './temp')

print(output_filter)
#print(output_filter)
#print(output_filter)
#exit(0)
# for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
#     if(op_i == ops[-1]):
#         output_filter_i = {None:output_filter}
#     else:
#         output_filter_i = generate_output_filter_from_previous(op_i, ops)
#     output_filter = AllOr(*list(output_filter_i.values()))
#     inference_i = op_i.get_inference_instance(output_filter)
#     inference_i.input_filters = generate_input_filters(op_i, inference_i, output_filter, output_schemas)
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
    while True:
        last_return, inference_i.input_filters = generate_input_filters_general(op_i, inference_i, output_filter, output_schemas, last_return)
        if inference_i.check_small_model() and inference_i.verify_correct():
            break
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    print(type(op_i))

    # assert(inference_i.check_small_model())
    # assert(inference_i.verify_correct())


check_pushdown_result(ops, 'temp/')