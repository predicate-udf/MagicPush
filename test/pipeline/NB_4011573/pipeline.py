
import sys
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

op0 = InitTable("data_0.pickle", '.pickle')
op1 = Rename(op0, {"0":"index"})
op9 = DropColumns(op1, ["4","9"])
op10 = DropColumns(op9, ["5","8"])
op11 = InitTable("data_28.pickle", '.pickle')
op12 = Rename(op11, {"0":"index"})
op13 = DropColumns(op12, ["6"])
op14 = Rename(op13, { "1":"Id","2":"College","3":"Course","4":"6","5":"7" })
op15 = Rename(op10, { "1":"Id","2":"College","3":"Course", '6':'6','7':'7'})
op16 = LeftOuterJoin(op15, op14, ['Id'], ['Id'])

op17 = SetItem(op16, "College_x", "lambda xxx__: str(xxx__['College_x']).replace('\\n','').replace('\\r','')" )
op18 = SetItem(op17, "College_y", "lambda xxx__: str(xxx__['College_y']).replace('\\n','').replace('\\r','')" )

ops = [op0, op1, op9, op10, op11, op12, op13, op14, op15, op16, op17, op18]
#ops = [op0, op1, op9, op10, op11, op12, op13, op14, op15, op16]

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
for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = {None:output_filter}
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    output_filter = AllOr(*list(output_filter_i.values()))
    inference_i = op_i.get_inference_instance(output_filter)
    inference_i.input_filters = generate_input_filters(op_i, inference_i, output_filter, output_schemas)
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    print(type(op_i))

    #print(inference_i.output_filter)


check_pushdown_result(ops, 'temp/')



# output_filter =  BinOp(Field('College_x'), '==', Constant(1))

# for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
#     if(op_i == ops[-1]):
#         output_filter_i = {None:output_filter}
#     else:
#         output_filter_i = generate_output_filter_from_previous(op_i, ops)
#     inference_i = op_i.get_inference_instance(output_filter_i)
#     inference_i.input_filters = generate_input_filters(op_i, inference_i, AllOr(*list(output_filter_i.values())))
#     # print(output_filter_i)
#     print(op_id, ':')
#     #print(inference_i)
#     print_input_filters(inference_i)
#     #print(inference_i.output_filter)

