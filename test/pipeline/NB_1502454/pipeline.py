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

op849368 = InitTable("data_187.pickle")
op2 = Rename(op849368, {"@OBS_VALUE":"US.LE_IX","@TIME_PERIOD":"date"})
op3 = Rename(op849368, {"@OBS_VALUE":"US.PCPI_IX","@TIME_PERIOD":"date"})
op4 = InnerJoin(op2, op3, ["date"],["date"])
op699519 = InitTable("data_403.pickle")
op5 = Rename(op699519, {"@OBS_VALUE":"US.ENDE_XDC_USD_RATE","@TIME_PERIOD":"date"})
op6 = InnerJoin(op4, op5, ["date"],["date"])
op545432 = InitTable("data_514.pickle")
op7 = Rename(op545432, {"@OBS_VALUE":"KR.LE_IX","@TIME_PERIOD":"date"})
op8 = InnerJoin(op6, op7, ["date"],["date"])
op104176 = InitTable("data_626.pickle")
op9 = Rename(op104176, {"@OBS_VALUE":"KR.PCPI_IX","@TIME_PERIOD":"date"})
op10 = InnerJoin(op8, op9, ["date"],["date"])
op541061 = InitTable("data_405.pickle")
op11 = Rename(op541061, {"@OBS_VALUE":"KR.ENDE_XDC_USD_RATE","@TIME_PERIOD":"date"})
op12 = InnerJoin(op10, op11, ["date"],["date"])
op429172 = InitTable("data_849.pickle")
op13 = Rename(op429172, {"@OBS_VALUE":"JP.LE_IX","@TIME_PERIOD":"date"})
op14 = InnerJoin(op12, op13, ["date"],["date"])
op15 = Rename(op545432, {"@OBS_VALUE":"JP.PCPI_IX","@TIME_PERIOD":"date"})
op16 = InnerJoin(op14, op15, ["date"],["date"])
op225098 = InitTable("data_1072.pickle")
op17 = Rename(op225098, {"@OBS_VALUE":"JP.ENDE_XDC_USD_RATE","@TIME_PERIOD":"date"})
op18 = InnerJoin(op16, op17, ["date"],["date"])
op764378 = InitTable("data_1184.pickle")
op19 = Rename(op764378, {"@OBS_VALUE":"RU.LE_IX","@TIME_PERIOD":"date"})
op20 = InnerJoin(op18, op19, ["date"],["date"])
op21 = Rename(op225098, {"@OBS_VALUE":"RU.PCPI_IX","@TIME_PERIOD":"date"})
op22 = InnerJoin(op20, op21, ["date"],["date"])
op544862 = InitTable("data_1407.pickle")
op23 = Rename(op544862, {"@OBS_VALUE":"RU.ENDE_XDC_USD_RATE","@TIME_PERIOD":"date"})
op24 = InnerJoin(op22, op23, ["date"],["date"])
op25 = DropColumns(op24, [])
op27 = InitTable("data_1416.pickle")
op28 = Filter(op27, IsNotNULL(Field("marketcap(USD)")))
op30 = DropNA(op28, ["date","marketcap(USD)"])
op32 = LeftOuterJoin(op30, op25, ["date"],["date"])
op33 = ChangeType(op32, "datetime", "date", "date")
op34 = Filter(op33, BinOp(Field("date"),'<',Constant("2016-06-01 00:00:00",typ='datetime')))

#op48 = DropNA(op34, ["date","marketcap(USD)","US.LE_IX","US.PCPI_IX","US.ENDE_XDC_USD_RATE","KR.LE_IX","KR.PCPI_IX","KR.ENDE_XDC_USD_RATE","JP.LE_IX","JP.PCPI_IX","JP.ENDE_XDC_USD_RATE","RU.LE_IX","RU.PCPI_IX","RU.ENDE_XDC_USD_RATE"])
op53 = Filter(op34, And(BinOp(Field("date"),'>=',Constant("2016-01-01 00:00:00", typ='datetime')),BinOp(Field("date"),'<',Constant("2016-06-01 00:00:00", typ='datetime'))))

ops = [op849368, op2, op3, op4, op699519, op5, op6, op545432, op7, op8, op104176, op9, op10, op541061, op11, op12, op429172, op13, op14, op15, op16, op225098, op17, op18, op764378, op19, op20, op21, op22, op544862, op23, op24, op25, op27, op28, op30, op32, op33, op34, op53]



output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = get_output_filter(ops, './temp')

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
#     inference_i.input_filters = generate_input_filters(op_i, inference_i, AllOr(*list(output_filter_i.values())))
#     # print(output_filter_i)
#     print(op_id, ':')
#     print_input_filters(inference_i)
#     #print(inference_i.output_filter)
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

check_pushdown_result(ops, 'temp/')
