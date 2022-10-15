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
op3 = LeftOuterJoin(op0, op1, ["id"],["id"])
op4 = Split(op3, 'categories', ["0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31","32","33","34","35"], None, by=";")
#op5 = ConcatColumn([op1,op4]) --- Concat cannot be pushed down; but if we rewrite to InnerJoin, then it can
op5 = InnerJoin(op1, op4, ['index'], ['index'])
op7 = DropColumns(op5, ["categories"])
op10 = Rename(op7, { "id":"id","0":"related","1":"request","2":"offer","3":"aid_related","4":"medical_help","5":"medical_products","6":"search_and_rescue","7":"security","8":"military","9":"child_alone","10":"water","11":"food","12":"shelter","13":"clothing","14":"money","15":"missing_people","16":"refugees","17":"death","18":"other_aid","19":"infrastructure_related","20":"transport","21":"buildings","22":"electricity","23":"tools","24":"hospitals","25":"shops","26":"aid_centers","27":"other_infrastructure","28":"weather_related","29":"floods","30":"storm","31":"fire","32":"earthquake","33":"cold","34":"other_weather","35":"direct_report" })
op11 = DropColumns(op3, ["categories"])


ops = [op0, op1, op3, op4]

output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')

output_filter = get_output_filter(ops, './temp')
print("The output filter is:")
print(output_filter)

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

    assert(inference_i.check_small_model())
    assert(inference_i.verify_correct())


check_pushdown_result(ops, 'temp/')

# output_filter = get_output_filter(ops, './temp')

# print(output_filter)
# #print(output_filter)
# #print(output_filter)
# #exit(0)
# for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
#     if(op_i == ops[-1]):
#         output_filter_i = {None:output_filter}
#     else:
#         output_filter_i = generate_output_filter_from_previous(op_i, ops)
#     inference_i = op_i.get_inference_instance(output_filter_i)
#     inference_i.input_filters = generate_input_filters_general(op_i, inference_i, AllOr(*list(output_filter_i.values())), output_schemas)
#     # print(output_filter_i)
#     print(op_id, ':')
#     print_input_filters(inference_i)
#     #print(inference_i.output_filter)

# check_pushdown_result(ops, 'temp/')