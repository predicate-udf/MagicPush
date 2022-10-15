
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

op804426 = InitTable("data_85.pickle")
#op12 = SortValues(op804426, ["2016"])
op13 = TopN(op804426, 20, ["2016"])
# op14 = DropColumns(op13, ['index'])
#op15 = SortValues(op804426, ["1960"])
op16 = TopN(op804426, 20, ['1960'])
# op17 = DropColumns(op16, ['index'])
#op19 = Append([op17,op14])
op19 = Append([op16,op13])
op20 = DropDuplicate(op19, ["Country Name","Country Code","1960","1961","1962","1963","1964","1965","1966","1967","1968","1969","1970","1971","1972","1973","1974","1975","1976","1977","1978","1979","1980","1981","1982","1983","1984","1985","1986","1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016"])
op21 = SortValues(op20, ["index"])

ops = [op804426,  op13, op16,  op19, op20, op21]


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

check_pushdown_result(ops, 'temp/')