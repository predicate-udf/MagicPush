

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


op2 = InitTable("data_0.pickle")

op3 = DropColumns(op2, ["created","score","url","title","num_comments","article","subreddit","id","comments"])
op4 = DropNA(op3, ["body"])
op5 = DropColumns(op2, ["created","score","url","body","title","num_comments","article","subreddit","id"])

op8 = DropColumns(op2, ["created","score","url","body","title","num_comments","subreddit","id","comments"])
op9 = DropNA(op8, ["article"])
op11 = InitTable("data_13.pickle")

op12 = DropColumns(op11, ["created","score","url","title","num_comments","article","subreddit","id","querry","comments"])
op13 = DropNA(op12, ["body"])
op14 = DropColumns(op11, ["created","score","url","body","title","num_comments","article","subreddit","id","querry"])

op17 = DropColumns(op11, ["created","score","url","body","title","num_comments","subreddit","id","querry","comments"])
op18 = DropNA(op17, ["article"])
op19 = Rename(op9, { "article":"text" })
op20 = Rename(op5, { "comments":"text" })
op21 = Rename(op4, { "body":"text" })
op22 = Rename(op18, { "article":"text" })
op23 = Rename(op14, { "comments":"text" })
op24 = Rename(op13, { "body":"text" })
op25 = Append([op19,op20,op21,op22,op23,op24])

ops = [op2, op3, op4, op5, op8, op9, op11, op12, op13, op14, op17, op18, op19, op20, op21, op22, op23, op24, op25]



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
    output_filter_i = AllOr(*list(output_filter_i.values()))
    inference_i = op_i.get_inference_instance(output_filter_i)
    inference_i.input_filters = generate_input_filters(op_i, inference_i, output_filter_i, output_schemas)
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    #print(inference_i.output_filter)
    print(type(op_i))
    assert(inference_i.check_small_model())
    #assert(inference_i.verify_correct())


check_pushdown_result(ops, 'temp/')
