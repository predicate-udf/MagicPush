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
op2 = DropColumns(op0, ["World Happiness Report 2016 (Cantril Ladder (0=worst; 10=best))"])
op8 = Filter(op2, BinOp(Field("Year"),'>',Constant(2006)))
op9 = Filter(op8, BinOp(Field('Entity'), 'in', Constant(["Albania","Argentina","Armenia","Australia","Azerbaijan","Bangladesh","Belarus","Belgium","Belize","Bolivia","Bosnia and Herzegovina","Brazil","Bulgaria","Burkina Faso","Cambodia","Cameroon","Canada","Central African Republic","Chad","Chile","China","Colombia","Costa Rica","Croatia","Czech Republic","Denmark","Dominican Republic","Ecuador","Egypt","El Salvador","Estonia","Georgia","Germany","Ghana","Greece","Guatemala","Guyana","Honduras","Hungary","India","Indonesia","Iran","Israel","Italy","Japan","Jordan","Kazakhstan","Kenya","Kosovo","Kyrgyzstan","Laos","Latvia","Liberia","Lithuania","Macedonia","Malawi","Malaysia","Mauritania","Mexico","Moldova","Mongolia","Montenegro","Mozambique","Namibia","Nepal","Netherlands","New Zealand","Nicaragua","Niger","Nigeria","Pakistan","Palestine","Panama","Paraguay","Peru","Philippines","Poland","Romania","Russia","Saudi Arabia","Senegal","Serbia","Sierra Leone","Singapore","South Africa","South Korea","Spain","Sri Lanka","Sweden","Tajikistan","Tanzania","Thailand","Turkey","Uganda","Ukraine","United Kingdom","United States","Uruguay","Vietnam","Yemen","Zambia","Zimbabwe"])))
op10 = GroupBy(op9, ["Entity"], { "Cantril_Score":(Value(0),"count"),"Year":(Value(0),"count"),"Code":(Value(0),"count") }, { "Cantril_Score":"Cantril_Score","Year":"Year","Code":"Code" })

op12 = Filter(op10, BinOp(Field("Code"),'==',Constant(12)))

ops = [op0, op2, op8, op9, op10, op12]

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
    inference_i.input_filters = generate_input_filters(op_i, inference_i, AllOr(*list(output_filter_i.values())))
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    #print(inference_i.output_filter)

check_pushdown_result(ops, 'temp/')