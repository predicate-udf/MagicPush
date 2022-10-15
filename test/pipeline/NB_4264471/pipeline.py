import sys
# from ppl_interface import *
sys.path.append("../../../")
sys.path.append("../../")
import z3
import dis
from interface import *
from util import *
import random
from constraint import *
from predicate import *
from generate_input_filters import *
from compare_pushdown_result import get_output_filter, check_pushdown_result
import os

op1 = InitTable("data_0.pickle")
op2 = DropColumns(op1, ["Events"])
op3 = DropColumns(op2, ['Unnamed: 10', 'Sea Level Press. (hPa)', 'Unnamed: 12', 'Unnamed: 13',
       'Visibility (km)', 'Unnamed: 15', 'Unnamed: 16', 'Wind (km/h)',
       'Unnamed: 18', 'Unnamed: 19', 'Precip. (mm)'])
op4 = DropColumns(op3, ['Dew Point (°C)', 'Unnamed: 6', 'Unnamed: 7', 'Humidity (%)'])
op5 = DropColumns(op4, ["2011"])
op7 = Rename(op5, { "Unnamed: 0":"date","Temp. (°C)":"highT","Unnamed: 3":"avgT","Unnamed: 4":"lowT","Unnamed: 9":"avgH" })
op8 = Filter(op7, IsNotNULL(Field('date')))
op9 = Rename(op8, {'date':'index'})
op10 = ChangeType(op9, 'int', 'highT','highT')
op11 = ChangeType(op10, 'int', 'avgT','avgT')
op12 = ChangeType(op11, 'int', 'lowT','lowT')
op13 = ChangeType(op12, 'int', 'avgH','avgH')
op14 = SetItem(op13, 'highT', 'lambda xxx__: (xxx__[\'highT\']*(9/5)+32)')
op15 = SetItem(op14, 'avgT', 'lambda xxx__: (xxx__[\'avgT\']*(9/5)+32)')
op16 = SetItem(op15, 'lowT', 'lambda xxx__: (xxx__[\'lowT\']*(9/5)+32)')


ops = [op1, op2,op3,op4, op5, op7, op8, op9, op10, op11, op12,op13, op14, op15, op16]
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
    print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    #print(inference_i.output_filter)

check_pushdown_result(ops, 'temp/')