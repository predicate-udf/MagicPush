
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

op0 = InitTable("data_0.pickle")
#op1 = DropColumns(op0, ["index"])
op1 = DropColumns(op0, ['26', '27'])
op2 = Rename(op1, { "0":"id","1":"cycle","2":"setting1","3":"setting2","4":"setting3","5":"s1","6":"s2","7":"s3","8":"s4","9":"s5","10":"s6","11":"s7","12":"s8","13":"s9","14":"s10","15":"s11","16":"s12","17":"s13","18":"s14","19":"s15","20":"s16","21":"s17","22":"s18","23":"s19","24":"s20","25":"s21" })
op3 = SortValues(op2, ["id","cycle"])
# op7 = InitTable("data_27.pickle")
#op8 = DropColumns(op7, ["index"])
# op8 = DropColumns(op7, ['26', '27'])
# op9 = Rename(op8, { "0":"id","1":"cycle","2":"setting1","3":"setting2","4":"setting3","5":"s1","6":"s2","7":"s3","8":"s4","9":"s5","10":"s6","11":"s7","12":"s8","13":"s9","14":"s10","15":"s11","16":"s12","17":"s13","18":"s14","19":"s15","20":"s16","21":"s17","22":"s18","23":"s19","24":"s20","25":"s21" })
# op11 = InitTable("data_32.pickle")
#op12 = DropColumns(op11, ["index"])
# op12 = DropColumns(op11, ['1'])
# op13 = Rename(op12, { "0":"more" })
op15 = GroupBy(op3, ["id"], { "cycle": (Value(0, False), 'max') }, { "cycle":"cycle" })
# op16 =  Rename(op3, { "cycle":"cycle_x" })
# op17 =  Rename(op15, { "cycle":"cycle_y" })
# rename op3, op15 cycle_x, cycle_y
# op18 = InnerJoin(op16, op17, ['id'], ['id'])
op18 = InnerJoin(op3, op15, ['id'], ['id'])
op19 = SetItem(op18, "RUL", 'lambda xxx__: (xxx__[\'cycle_y\']-xxx__[\'cycle_x\'])' )
op20= DropColumns(op19, ['cycle_y'])




ops = [op0, op1, op2, op3, op7, op8, op9, op11, op12, op13, op15,  op18, op19, op20]



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
    inference_i = op_i.get_inference_instance(output_filter_i)
    inference_i.input_filters = generate_input_filters(op_i, inference_i, AllOr(*list(output_filter_i.values())))
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    #print(inference_i.output_filter)

check_pushdown_result(ops, 'temp/')


