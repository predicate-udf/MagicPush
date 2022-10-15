
import sys
# from ppl_interface import *
sys.path.append("../../../")
sys.path.append("../../")
import z3
import dis
from interface import *
from util import *
import random
from constraint import *
from predicate import *
from generate_input_filters import *
from compare_pushdown_result import get_output_filter, check_pushdown_result
import os

op0 = InitTable("data_0.pickle")
op3 = Copy(op0)
op4 = Filter(op3, BinOp(Field("hotel_id"),'==',Constant(21)))
op5 = Filter(op3, BinOp(Field("hotel_id"),'!=',Constant(21)))
op6 = Append([op4,op5])
op7 = SetItem(op6, "similarity_distance", 'lambda xxx__:xxx__[\'distance\']*xxx__[\'distance\']')
op19 = SortValues(op7, ["similarity_distance"])
op20 = SortValues(op19, ["index"])

ops = [op0, op3, op4, op5, op6, op7, op19, op20]

output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = get_output_filter(ops, './temp')
print("The output filter is")
print(output_filter)
#print(output_filter)
#print(output_filter)
#exit(0)
for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = {None:output_filter}
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    inference_i = op_i.get_inference_instance(output_filter_i)
    inference_i.input_filters = generate_input_filters(op_i, inference_i, AllOr(*list(output_filter_i.values())))
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    #print(inference_i.output_filter)

check_pushdown_result(ops, 'temp/')