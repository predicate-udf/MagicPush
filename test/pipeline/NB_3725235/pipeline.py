
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
op1 = InitTable("data_1.pickle")
op2 = InitTable("data_2.pickle")
op3 = InitTable("data_3.pickle")
op4 = InitTable("data_4.pickle")
op5 = ChangeType(op0, "datetime", "Date", "Date")
op6 = ChangeType(op1, "datetime", "Date", "Date")
op7 = ChangeType(op2, "datetime", "Date", "Date")
op8 = ChangeType(op3, "datetime", "Date", "Date")
op9 = ChangeType(op4, "datetime", "Date", "Date")
op10 = Append([op5, op6, op7, op8, op9])

ops = [op0, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10]
output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = get_output_filter(ops, './temp')

print(output_filter)

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
