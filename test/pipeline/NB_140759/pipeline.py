

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
op00 = Rename(op0, {'file_name':'index'})
op0_0 = SetItem(op00, "hair_color", 'lambda xxx__: { -1:0 }[xxx__["hair_color"]] if xxx__["hair_color"] in [-1] else  xxx__["hair_color"]')
op0_1 = SetItem(op0_0, "eyeglasses", 'lambda xxx__: { -1:0 }[xxx__["eyeglasses"]] if xxx__["eyeglasses"] in [-1] else  xxx__["eyeglasses"]')
op0_2 = SetItem(op0_1, "smiling", 'lambda xxx__: { -1:0 }[xxx__["smiling"]] if xxx__["smiling"] in [-1] else  xxx__["smiling"]')
op0_3 = SetItem(op0_2, "young", 'lambda xxx__: { -1:0 }[xxx__["young"]] if xxx__["young"] in [-1] else  xxx__["young"]')
op2 = SetItem(op0_3, "human", 'lambda xxx__: { -1:0 }[xxx__["human"]] if xxx__["human"] in [-1] else  xxx__["human"]')
op3 = InitTable("data_1.pickle")
op8 = Rename(op3, {'file_name':'index'})
op9 = DropColumns(op2, ['hair_color', 'eyeglasses', 'smiling', 'human'])
op10 = InnerJoin(op8, op9, 'index', 'index')
op16 = Filter(op10, And(BinOp(Field("partition"),'==',Constant(0)),BinOp(Field("young"),'==',Constant(0))))

op18 = Filter(op10, And(BinOp(Field("partition"),'==',Constant(0)),BinOp(Field("young"),'==',Constant(1))))

op20 = Append([op16,op18])

ops = [op0, op00, op0_0, op0_1, op0_2, op0_3, op2, op3, op8, op9, op10, op16, op18, op20]

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
    output_filter = AllOr(*list(output_filter_i.values()))
    inference_i = op_i.get_inference_instance(output_filter)
    inference_i.input_filters = generate_input_filters(op_i, inference_i, output_filter)
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    print(type(op_i))
    assert(inference_i.check_small_model())
    assert(inference_i.verify_correct())

check_pushdown_result(ops, 'temp/')