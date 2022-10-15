
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


op10 = InitTable("data_9.pickle")
op11 = DropColumns(op10, [])
op12 = Rename(op11, { "Cumulative cases of COVID-19 in the U.S. from January 22 to May 8, 2020, by day":"Date","Unnamed: 1":"Cumulative_Cases" })
op13 = SortValues(op12, ["index"])
op13_0 = DropNA(op13, ["Cumulative_Cases"])
op14 = SetItem(op13_0, "Cumulative_Cases", 'lambda xxx__: str(xxx__["Cumulative_Cases"]).replace(",","")')
#op15 = ChangeType(op13, "int", "Cumulative_Cases", "Cumulative_Cases")

# Yin: there is an error in the generation code
op15 = ChangeType(op14, "str", "Cumulative_Cases", "Cumulative_Cases")



ops = [op10, op11, op12, op13, op13_0, op14, op15]


output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = get_output_filter(ops, './temp')
print("The output_filter is:")
print(output_filter)
#print(output_filter)
#print(output_filter)
#exit(0)
for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = {None:output_filter}
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    output_filter_i = AllOr(*list(output_filter_i.values()))
    inference_i = op_i.get_inference_instance(output_filter_i)
    inference_i.input_filters = generate_input_filters(op_i, inference_i, output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    #print(inference_i.output_filter)
    print(type(op_i))
    assert(inference_i.check_small_model())
    assert(inference_i.verify_correct())

check_pushdown_result(ops, 'temp/')