
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
op1 = DropColumns(op0, ["Critic_Score"])
op2 = DropColumns(op1, ["Critic_Count"])
op3 = DropColumns(op2, ["User_Score"])
op4 = DropColumns(op3, ["User_Count"])
op5 = DropColumns(op4, ["Rating"])
op6 = DropColumns(op5, ["Developer"])
op7 = DropNA(op6, ["Name","Platform","Year_of_Release","Genre","Publisher","NA_Sales","EU_Sales","JP_Sales","Other_Sales","Global_Sales"])
op11 = SetItem(op7, 'Rank',"lambda xxx__:xxx__['index']")
op12 = SetItem(op11, "NA_Sales_Adj", 'lambda xxx__: (xxx__["NA_Sales"] / 579)' )

ops = [op0, op1, op2, op3, op4, op5, op6, op7, op11, op12]

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
    inference_i = op_i.get_inference_instance(output_filter_i)
    inference_i.input_filters = generate_input_filters(op_i, inference_i, AllOr(*list(output_filter_i.values())), output_schemas)
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    #print(inference_i.output_filter)

check_pushdown_result(ops, 'temp/')