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
op1_0 = FillNA(op1, "age", 0)
op1_1 = FillNA(op1_0, "gender", 0)
op1_2 = FillNA(op1_1, "race", 0)
op1_3 = FillNA(op1_2, "ethnicity", 0)
op1_4 = FillNA(op1_3, "employment_status", 0)
op1_5 = FillNA(op1_4, "hours_worked_per_week", 0)
op17 = FillNA(op1_5, "earnings_per_week", 0)
op23 = Filter(op17, BinOp(Field("hours_worked_per_week"),'>',Constant(0)))
op24 = Copy(op23)
op28 = SetItem(op24, "female", 'lambda xxx__: { "Female":1,"Male":0 }[xxx__["gender"]] if xxx__["gender"] in ["Female","Male"] else xxx__["gender"]','int')
op30 = SetItem(op28, "person_hard_worker", 'lambda xxx__: 1 if xxx__[\'hours_worked_per_week\'] > 38.8 else 0')
#ops = [op1, op1_0, op1_1, op1_2, op1_3, op1_4, op1_5, op17, op23, op24, op28, op30]
ops = [op1, op1_0, op1_1, op1_2, op1_3, op1_4, op1_5, op17, op23, op24, op28, op30]
output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = get_output_filter(ops, './temp')
#output_filter = BinOp(Field('age'), '==', Constant(32))
#output_filter = And(BinOp(Field('hours_worked_per_week'), '>', Constant(10)), BinOp(Field('female'), '==', Constant(0)))

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