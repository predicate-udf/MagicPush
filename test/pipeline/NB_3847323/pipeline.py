import sys
# from ppl_interface import *
sys.path.append("../../../")
sys.path.append("../../")
import z3
import dis
from interface import *
from util import *
import random
# from constraint import *
from predicate import *
from generate_input_filters import *
from compare_pushdown_result import get_output_filter, check_pushdown_result, get_output_filter_all_operators
import os
from table_constraint import *

op0 = InitTable("data_0.pickle")
op1 = Rename(op0, { "{8355F008-C0A9-55C5-E053-6B04A8C0D090}":"TUID","102000":"Price","2003-11-25 00:00":"Date_Transfer","WA5 2PG":"Postcode","S":"Prop_Type","N":"Old_New","L":"Duration","39":"PAON","Unnamed: 8":"SAON","ROTHAY DRIVE":"Street","PENKETH":"Locality","WARRINGTON":"Town_City","WARRINGTON.1":"District","WARRINGTON.2":"County","A":"PPD_Cat_Type","A.1":"Record_Status" })
op5 = SortValues(op1, ["Date_Transfer"])
op6 = Filter(op5, BinOp(Field('Town_City'), '==', Constant('LONDON')))
op8 = GroupBy(op6, ["Street"], { "Price":(Value(0, True),"mean") }, { "Price":"Price" })

op10 = Rename(op8, { "Street":"Street","Price":"Avg_Price" })
op11 = Filter(op10, And(BinOp(Field('Avg_Price'), '>=', Constant(2000000)), BinOp(Field('Avg_Price'), '<=', Constant(3000000))))


ops = [op0, op1, op5, op6, op8, op10, op11]

# add functional dependency

for op in ops:
    get_constraint(op)

#op8.constraints.append(FunctionalDependency('Price', ['Street'], 'lambda x: 2500000 if x["Street"] == \'NORTH SQUARE\' else 0'))
# op8.append(DomainConstraint({'Avg_Price', set(2500000)}))

# for o in op8.constraints:
#     #if isinstance(o, DomainConstraint):
#     print(o)
# exit()
output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


#output_filter = get_output_filter(ops, './temp')

#output_filter = And(BinOp(Field('Street'), '==', Constant('NORTH SQUARE')), BinOp(Field('Avg_Price'), '==', Constant(2250000)))
output_filter = Or(BinOp(Field('Street'), '==', Constant('NORTH SQUARE')), BinOp(Field('Avg_Price'), '==', Constant(2500000)))
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
    output_filter = AllOr(*list(output_filter_i.values()))
    inference_i = op_i.get_inference_instance(output_filter)
    last_return = None
    rewrite = False
    snapshot = False
    while True:
        last_return, inference_i.input_filters = generate_input_filters_general(op_i, inference_i, output_filter, output_schemas, last_return)
        if inference_i.check_small_model() and inference_i.verify_correct():
            break
        # have searched everything but none of them works
        if last_return == 'remove' or last_return == 'join' or last_return == 'multi':
            print("Cannot pushdown, try output_filter rewrite or snapshot.")
            rewrite = True
            snapshot = True
            break
    # try output filter rewrite
    if rewrite:
        trials = 0
        print("Try output filter rewrite")
        while True:
            trials, output_filter_new = output_filter_rewrite(output_schemas, output_filter, op_i, trials)
            if len(output_filter_new) != 0:
                # len(output_filter_new) == 2 for join situation
                inference_i.input_filters = output_filter_new
                snapshot = False
                break
            if trials == -1:
                break
    # try take a snapshot
    if snapshot:
        print("Output filter rewrite failed, need to take a snapshot.")
        inference_i.input_filters = snapshot_generate_predicate(output_schemas, op_i, output_filter)
    assert(inference_i.check_small_model())
    assert(inference_i.verify_correct())
    print(op_id, ':')
    print_input_filters(inference_i)



# for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
#     if(op_i == ops[-1]):
#         output_filter_i = {None:output_filter}
#     else:
#         output_filter_i = generate_output_filter_from_previous(op_i, ops)
#     output_filter = AllOr(*list(output_filter_i.values()))
#     inference_i = op_i.get_inference_instance(output_filter)
#     inference_i.input_filters = generate_input_filters(op_i, inference_i, output_filter, output_schemas)
#     # print(output_filter_i)
#     print(op_id, ':')
#     print_input_filters(inference_i)
#     print(type(op_i))
#     assert(inference_i.check_small_model())
#     assert(inference_i.verify_correct())


# for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
#     if(op_i == ops[-1]):
#         output_filter_i = {None:output_filter}
#     else:
#         output_filter_i = generate_output_filter_from_previous(op_i, ops)
#     inference_i = op_i.get_inference_instance(output_filter_i)
#     # making use of constraints
#     #get_constraint(op_i)

#     inference_i.input_filters = generate_input_filters(op_i, inference_i, AllOr(*list(output_filter_i.values())), output_schemas)
#     # print(output_filter_i)
#     print(op_id, ':')
#     print_input_filters(inference_i)
#     #print(inference_i.output_filter)

check_pushdown_result(ops, 'temp/')