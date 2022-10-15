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

op10 = InitTable("data_22.pickle")
op11 = UnPivot(op10, ["country","year"], ["m4554","mu","m2534","m1524","m3544","m014","m5564","m65","f014"], "sex_and_age", "cases")
op12_0 = Split(op11, "sex_and_age", ["0","1","2"], "(\D)(\d+)(\d{2})")
#op12 = DropColumns(op12_0, ['country','year','cases'])
op13 = Rename(op12_0, { "0":"sex","1":"age_lower","2":"age_upper" })
op14 = SetItem(op13, 'age', "lambda xxx__: str(xxx__['age_lower'])+'-'+str(xxx__['age_upper'])", 'str')
#op15 = ConcatColumn([op11,op13])
op15 = InnerJoin(op11, op14, ['index'], ['index'])
op16 = DropColumns(op15, ["sex_and_age","age_lower","age_upper"])
op17 = DropNA(op16, ["country","year","cases","sex","age"])
op18 = SortValues(op17, ["country","year","sex","age"])

ops = [op10, op11, op12, op13, op15, op16, op17, op18]
output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = get_output_filter(ops, './temp')
print("The output filter is:")
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