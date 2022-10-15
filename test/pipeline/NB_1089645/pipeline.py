
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

op1 = InitTable("data_1.pickle")
op3 = DropColumns(op1, ["imdb_id","homepage","tagline","keywords","overview","production_companies","budget_adj","revenue_adj"])
op4 = DropDuplicate(op3, ["id","popularity","budget","revenue","original_title","cast","director","runtime","genres","release_date","vote_count","vote_average","release_year"])
op14 = DropColumns(op4, ["id","runtime","vote_count","release_date"])
op15 = SortValues(op14, ["popularity"])
op16 = SetItem(op15, 'new_column', 'lambda xxx__:xxx__[\'revenue\']-xxx__[\'budget\']')

ops = [op1, op3, op4, op14, op15, op16]


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