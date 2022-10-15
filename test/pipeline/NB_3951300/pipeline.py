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
from compare_pushdown_result import get_output_filter, check_pushdown_result, get_output_filter_all_operators
import os

op0 = InitTable("data_0.pickle")
op2 = Rename(op0, { "SNo":"sno","ObservationDate":"observationdate","Province/State":"province/state","Country/Region":"country/region","Last Update":"last update","Confirmed":"confirmed","Deaths":"deaths","Recovered":"recovered" })
#op3 = Rename(op2, { "sno":"sno","observationdate":"observationdate","province/state":"province/state","country/region":"country/region","last update":"last_update","confirmed":"confirmed","deaths":"deaths","recovered":"recovered" })
op10 = SetItem(op2, "country/region", 'lambda xxx__: xxx__["country/region"].lstrip()')
op15 = Filter(op10, And(BinOp(Field("country/region"),'!=',Constant("Others")),BinOp(Field("country/region"),'!=',Constant("Diamond Princess"))))
op16 = Filter(op15, BinOp(Field("country/region"),'!=',Constant("MS Zaandam")))
op17 = Filter(op16, BinOp(Field("country/region"),'!=',Constant("Kosovo")))
op18 = Filter(op17, BinOp(Field("country/region"),'!=',Constant("Holy See")))
op19 = Filter(op18, BinOp(Field("country/region"),'!=',Constant("Vatican City")))
op20 = Filter(op19, BinOp(Field("country/region"),'!=',Constant("Timor-Leste")))
op21 = Filter(op20, BinOp(Field("country/region"),'!=',Constant("East Timor")))
op22 = Filter(op21, BinOp(Field("country/region"),'!=',Constant("Channel Islands")))
op23 = Filter(op22, BinOp(Field("country/region"),'!=',Constant("Western Sahara")))
op33 = GroupBy(op23, ["observationdate","country/region"], { "confirmed":(Value(0, False),"sum") }, { "confirmed":"confirmed" })
op35 = Filter(op33, BinOp(Field("confirmed"),'>=',Constant(100)))
op36 = SortValues(op35, ["observationdate"])
op37 = DropDuplicate(op36, ["country/region"])
#op38 = SortValues(op37, ["index"])

ops = [op0, op2, op3, op10, op15, op16, op17, op18, op19, op20, op21, op22, op23, op33, op35, op36, op37]#, op38]
output_schemas = generate_output_schemas(ops)


for op in ops:
    get_constraint(op)
    print("The filter for {} is".format(op))
    print(op.constraints)
    
def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = get_output_filter_all_operators(ops, './temp')
#output_filter1 = get_output_filter_all_operators(ops, './temp')
print("The output filter is:")
print(output_filter)

#print(output_filter)
#print(output_filter)
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
