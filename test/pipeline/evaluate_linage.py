
import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")

import z3
import dis
from interface import *
from util import *
import random
# from constraint import *
from predicate import *
from generate_input_filters import generate_input_filters_general, generate_output_filter_from_previous, output_filter_rewrite, snapshot_generate_predicate
from compare_pushdown_result import get_output_filter, check_pushdown_result, get_output_filter_all_operators
import os
from table_constraint import *
from input_filter_baseline import get_input_filter_baseline
from eval_util import *
import numpy as np
import re
import functools
import time


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
NB_path = sys.argv[1]
mkdir(NB_path + '/temp')
pipe_code, ops = read_pipeline_code(NB_path)
pipe_code.append("ops = [{}]".format(','.join(ops)))
# print(''.join(pipe_code))
exec(''.join(pipe_code))
output_schemas = generate_output_schemas(ops)
for op in ops:
    get_constraint(op)
#output_filter = Or(BinOp(Field('Street'), '==', Constant('NORTH SQUARE')), BinOp(Field('Avg_Price'), '==', Constant(2500000)))
output_filter = get_output_filter(ops, './temp')
pipeline_output_filter = output_filter
print("The output_filter is")
print(output_filter)

is_superset = []
use_baseline = False
assertion=False
debug = False
if len(sys.argv) > 2:
    debug=True



start_time = time.time()
for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = {None:output_filter}
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    output_filter = AllOr(*list(output_filter_i.values()))
    inference = op_i.get_inference_instance(output_filter)
    if debug:
        print("output filter for {} is {}".format(type(inference), inference.output_filter))
    last_return = None
    rewrite = False
    snapshot = False
    if use_baseline:
        #print(type(inference))
        inference.input_filters = get_input_filter_baseline(output_filter, op_i, inference, assertion)
        #print(inference.input_filters[0])
    else:
        last_filters = []
        while True:
            last_return, inference.input_filters = generate_input_filters_general(op_i, inference, output_filter, output_schemas, last_return)
            #print(last_return, inference.input_filters)
            if debug:
                print("output filter of {} = {}".format(type(op_i), inference.output_filter))
                print("To verify {}".format(inference.input_filters[0]))
            if any([all([str(history[i])==str(inference.input_filters[i]) for i in range(len(inference.input_filters))]) for history in last_filters]):
                inference.input_filters = [True for i in range(len(inference.input_filters))]
                print("CANNOT PUSHDOWN， TRY FILTER REWRITE OR SNAPSHOT.")
                rewrite = True
                snapshot = True
                break
            last_filters.append(inference.input_filters)
            #print("To verify {}".format(type(op_i)))
            #print(inference.input_filters[0])
            #for pred_i,pred in enumerate(inference.input_filters):
            #    print("filter {} = {}".format(pred_i, pred))
            if inference.check_small_model() and inference.verify_correct():
                if debug:
                    print("The correct input filter is")
                    print(inference.input_filters[0])
                break
            if inference.check_small_model(check_superset=True) and inference.verify_correct(check_superset=True):
                # if input_filter == True
                # if not check_predicate_equal(inference.input_filters[0], True, inference.input_tables[0]):
                #print("The correct superset pushdown is")
                #print(inference.input_filters[0])
                # TODO: determine whether to take a snapshot
                if debug:
                    print("Break because of superset")
                    print(inference.input_filters[0])
                is_superset.append(type(op_i))
                break
        if rewrite:
            trials = 0
            print("Try output filter rewrite")
            while True:
                trials, output_filter_new = output_filter_rewrite(output_schemas, output_filter, op_i, trials)
                if len(output_filter_new) != 0:
                    # len(output_filter_new) == 2 for join situation
                    inference.input_filters = output_filter_new
                    snapshot = False
                    break
                if trials == -1:
                    break
        # try take a snapshot
        if snapshot:
            print("Output filter rewrite failed, need to take a snapshot.")
            inference.input_filters = snapshot_generate_predicate(output_schemas, op_i, output_filter)
        if not (inference.check_small_model() and inference.verify_correct()):
            if not (inference.check_small_model(check_superset=True) and inference.verify_correct(check_superset=True)):
                print("CANNOT PUSHDOWN")
                exit()
def simplify_input_filter(op):
    ret = []
    tables = op.run_input_filter(op.input_tables, op.input_filters)
    for table in tables:
        ret.append(z3.simplify(table[0].eval_exist_cond()))
    return ret


end_time = time.time()

print("The time for data debugging is")
print(end_time - start_time)
import numpy as np
def evaluate(op):
    # print(op.inference.input_filters[0])
    s = op.to_python('df')
    s += "\nprint('orig size = {}'.format(df.shape[0]))\n"
    s += "{} = {}[{}.apply(lambda row: {}, axis=1)]".format('df', 'df', 'df', pred_to_python_using_lambda(op.inference.input_filters[0], 'df'))
    s += "\nprint('reduced size = {}'.format(df.shape[0]))\n"
    s = s.replace("''","'")
    # print(s)
    try:
        exec(s)
    except:
        print("FAILS")

print("\n")

from lineage_tracking_baseline import *
from compare_pushdown_result import get_operator_output_variable_name

print_lineage_code = """
rows_by_table = {i+1:set() for i in range(%d)}
for idx,row in final_output.iterrows():
    for pair in row['lineage_tracking']:
        rows_by_table[pair[0]].add(pair[1])
table_names = [%s]
for tableid in range(%d):
    print("table {} select {} rows".format(table_names[tableid], len(rows_by_table[tableid+1])))
"""

def generate_script_with_lineage_tracking(ops, output_filter):
    #print("OUTPUT FILTER : {}".format(output_filter))
    lines = []
    input_variable_names = {}
    table_id = 0
    table_names = []
    final_output_var = ''
    for i,op in enumerate(ops):
        if isinstance(op, InitTable):
            table_id += 1
            table_names.append(op.datafile_path)
        output_var = get_operator_output_variable_name(i)
        input_variable_names[op] = output_var
        lines.append(pandas_code_with_lineage_tracking_oneop(op, output_var, input_variable_names, table_id))
        final_output_var = output_var
    lines.append("final_output = {}[{}.apply(lambda row: {}, axis=1)]\n".format(final_output_var, final_output_var, pred_to_python_using_lambda(output_filter, output_var)))
    lines.append("print('final_output = {}'.format(final_output.head()))\n")
    lines.append(print_lineage_code % (table_id, ','.join(['"{}"'.format(t) for t in table_names]),table_id))
    code = groupby_sum_code + "\n" + '\n'.join(lines)
    #print(code)
    exec(code)

for op in ops:
    if isinstance(op, InitTable):
        # print("------------")
        # print("FILTER ON INPUT TABLE: {} {} {}".format(op.datafile_path, op.inference.input_filters[0], '/ superset ({})'.format(is_superset) if len(is_superset)>0 else ''))
        #print("simplified = {}".format(simplify_input_filter(op.inference)[0]))
        evaluate(op)

start_2 = time.time()

print("........................baseline......................................")
generate_script_with_lineage_tracking(ops, pipeline_output_filter)

end_2 = time.time()
print("The time for baseline is")
print(end_2-start_2)