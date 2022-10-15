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
op3 = Filter(op0, BinOp(Field("language"),'==',Constant("english")))
op4 = Filter(op3, BinOp(Field("country"),'==',Constant("US")))
op6 = DropColumns(op4, ["main_img_url","crawled","replies_count","likes","spam_score","site_url","language","ord_in_thread","shares","uuid","country","comments","participants_count","domain_rank"])
op7 = SetItem(op6, "word_count", 'lambda xxx__: len(xxx__["text"].split(" "))')
op8 = SetItem(op7, "published", 'lambda xxx__: xxx__["published"][0:10]')
op9 = Filter(op8, AllOr(*[BinOp(Field("published"),'==',Constant("2016-10-26")),BinOp(Field("published"),'==',Constant("2016-10-27"))]))
op12 = DropDuplicate(op9, ["title","text"])
op13 = Filter(op12, BinOp(Field("word_count"),'>',Constant(200)))
op14 = Filter(op13, Not(IsNULL(Field("title"))))
op15 = SortValues(op14, ["index"])

op19 = DropNA(op15, ["text","author"])
op20 = DropDuplicate(op19, ["text"])

ops = [op0, op3, op4, op6, op7, op8, op9, op12, op13, op14, op15, op19, op20]

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