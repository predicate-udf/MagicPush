from predicate import *
import sys
import z3
import dis
import pandas as pd
import random
from util import *
from interface import *
from util_type import *
from predicate import *
# from constraint import *
from pandas_op import *
import pickle
import numpy as np


def read_original_code(path, ops):
    lines = []
    for line in open(path):
        if len(line) <= 2:
            continue
        if 'import ' in line:
            continue
        if line.lstrip().startswith('#'):
            continue
        line = line.replace('\n','')
        if '=' in line:
            chs = line.split('=')
            keyword = chs[1].lstrip()
        else:
            keyword = line
        if keyword.startswith('pickle.load') or keyword.startswith('pd.read_csv'):
            lines.append((line, 'readfile'))
        elif line[-1] == ']' and '[[' not in line:
            lines.append((line, 'filter'))
        else:
            lines.append((line, ''))
    
    ret = []
    for line,typ in lines:
        if typ == 'readfile':
            for op in ops:
                if isinstance(op, InitTable) and op.datafile_path.split('/')[-1] in line:
                    ret.append((line, op))
    for line,typ in lines:
        if typ == 'readfile':
            continue
        ret.append((line, typ))
    # for line,typ in ret:
    #     print(line)
    #     print(typ)
    # exit(0)
    return ret

prefix = """
import pandas as pd
import pickle
import time
import sys
"""

from parse_z3_expr import convert_z3_expr,z3_expr_requires_lambda
def simplify_input_filter(op, output_var):
    tables = op.run_input_filter(op.input_tables, op.input_filters)
    ret = None
    for table in tables:
        print(table[0].exist_cond)
        expr = z3.simplify(table[0].eval_exist_cond())
        use_lambda = z3_expr_requires_lambda(expr)
        print("BEFORE: {} (use_lambda={})".format(expr, use_lambda))
        r = convert_z3_expr(expr, use_lambda)
        print(r)
        if use_lambda:
            ret = "{} = {}[{}.apply(lambda xxx: {}, axis=1)]".format(output_var, output_var, output_var, r)
        else:
            ret = "{} = {}[{}]".format(output_var, output_var, pred_to_python(r, output_var))
    return ret



def generate_script_with_filter_on_input(orig_code, ops, orig_op_name, is_superset, new_path):
    lines = []
    lines.append("start1 = time.time()\n")
    for line,typ in orig_code:
        if isinstance(typ, InitTable):
            op = typ
            df_var = line.split('=')[0].lstrip().rstrip()
            lines.append("if len(sys.argv) > 1:")
            s = "\t{} = pickle.load(open(\"{}\",'rb'))\n".format(df_var, os.path.join(new_path, 'data/scaled_{}'.format(op.datafile_path.split('/')[-1])))
            lines.append(s)
            lines.append('else:')
            lines.append('\t'+line.replace(op.datafile_path.split('/')[-1], 'data/{}'.format(op.datafile_path.split('/')[-1])))
            lines.append("# {}".format(op.inference.input_filters[0]))
            lines.append(simplify_input_filter(op.inference, df_var))
    lines.append("start2 = time.time()\n")
    for line,typ in orig_code:
        if isinstance(typ, InitTable):
            continue
        elif type(typ) is str and typ == 'filter':
            if is_superset:
                lines.append(line)
            else:
                df_var = line.split('=')[0].lstrip().rstrip()
                orig_var = line.split('=')[1].split('[')[0].lstrip().rstrip()
                if df_var != orig_var:
                    lines.append('{} = {}\n'.format(df_var, orig_var))
        else:
            lines.append(line)

    opname_op_map = {op:orig_op_name[op_id] for op_id,op in enumerate(ops)}
    for op_id,op in enumerate(ops):
        if type(op.inference.output_filter) is dict: # op4 -> op5(True)/op7(totalStar==1)
            # op4.output_filter = {op5: True, op7: totalStar==1}
            # op4.input_filter = True
            lines.append("# OUTPUT BRANCH: {}({})\n".format(orig_op_name[op_id], type(op)))
            for nextop,f in op.inference.output_filter.items():
                lines.append("#     --- branch to {} : {}".format(opname_op_map[nextop], f))
    lines.append("ends = time.time()\n")
    lines.append("print('with loading: {} sec; without loading: {} sec'.format(ends-start1, ends-start2))\n")
    return prefix+'\n'+'\n'.join(lines)    

def generate_script_with_no_filter(orig_code, new_path=''):
    lines = []
    lines.append("start1 = time.time()\n")
    for line,typ in orig_code:
        if isinstance(typ, InitTable):
            op = typ
            df_var = line.split('=')[0].lstrip().rstrip()
            lines.append("if len(sys.argv) > 1:")
            s = "\t{} = pickle.load(open(\"{}\",'rb'))\n".format(df_var, os.path.join(new_path, 'data/scaled_{}'.format(op.datafile_path.split('/')[-1])))
            lines.append(s)
            lines.append('else:')
            lines.append('\t'+line.replace(op.datafile_path.split('/')[-1], 'data/{}'.format(op.datafile_path.split('/')[-1])))
    lines.append("start2 = time.time()\n")
    for line,typ in orig_code:
        if not isinstance(typ, InitTable):
            lines.append(line)
    lines.append("ends = time.time()\n")
    lines.append("print('with loading: {} sec; without loading: {} sec'.format(ends-start1, ends-start2))\n")
    return prefix+'\n'+'\n'.join(lines)


"""
def get_operator_output_variable_name(i):
    return 'df{}'.format(i)
def generate_script_with_filter_on_input_backup(ops, is_superset, run_on_scaled=False, new_path=''):
    lines = []
    input_variable_names = {}
    multiple_output_ops = {} # key: op, value: {nextop: var_name}
    lines.append("start1 = time.time()\n")
    for i,op in enumerate(ops):
        if isinstance(op, InitTable):
            output_var = get_operator_output_variable_name(i)
            input_variable_names[op] = output_var
            f = op.inference.input_filters[0]
            #lines.append("{} = {}[{}.apply(lambda row: {}, axis=1)]".format(output_var, output_var, output_var, pred_to_python_using_lambda(f, output_var)))
            lines.append("# {}\n".format(f))
            lines.append(op.to_python(output_var, input_variable_names))
            lines.append(simplify_input_filter(op.inference, output_var))
    lines.append("start2 = time.time()\n")
    for i,op in enumerate(ops):
        if isinstance(op, InitTable):
            continue
        output_var = get_operator_output_variable_name(i)
        input_variable_names[op] = output_var
        if any([ix in multiple_output_ops for ix in op.dependent_ops]):
            for ix in op.dependent_ops:
                if ix in multiple_output_ops:
                    input_variable_names[ix] = multiple_output_ops[ix][op]
        if isinstance(op, Filter) and is_superset==False:
            lines.append("{} = {}\n".format(output_var, input_variable_names[op.dependent_ops[0]]))
        else:
            lines.append(op.to_python(output_var, input_variable_names))
        #if op in superset:
        #    lines.append("# SUPERSET! \n")
        #    lines.append("# {} = {}[{}.apply(lambda row: {}, axis=1)]".format(output_var, output_var, output_var, pred_to_python_using_lambda(op.inference.output_filter, output_var)))
           
        if type(op.inference.output_filter) is dict: # op4 -> op5(True)/op7(totalStar==1)
            # op4.output_filter = {op5: True, op7: totalStar==1}
            # op4.input_filter = True
            j = 0
            multiple_output_ops[op] = {}
            for nextop,f in op.inference.output_filter.items():
                new_var = '{}_{}'.format(output_var, j)
                j += 1
                multiple_output_ops[op][nextop] = new_var
                lines.append("{} = {}[{}.apply(lambda row: {}, axis=1)]".format(new_var, output_var, output_var, pred_to_python_using_lambda(f, output_var)))
    lines.append("ends = time.time()\n")
    lines.append("print('with loading: {} sec; without loading: {} sec'.format(ends-start1, ends-start2))\n")
    return prefix+'\n'+'\n'.join(lines)    


def generate_script_with_no_filter_backup(ops, run_on_scaled=False, new_path=''):
    lines = []
    input_variable_names = {}
    lines.append("start1 = time.time()\n")
    for i,op in enumerate(ops):
        output_var = get_operator_output_variable_name(i)
        input_variable_names[op] = output_var
        if isinstance(op, InitTable):
            if run_on_scaled:
                s = "{} = pickle.load(open(\"{}\",'rb'))\n".format(output_var, os.path.join(new_path, 'data/{}'.format(op.datafile_path.split('/')[-1])))
                lines.append(s)
            else:
                lines.append(op.to_python(output_var, input_variable_names))
    lines.append("start2 = time.time()\n")
    for i,op in enumerate(ops):
        if isinstance(op, InitTable):
            continue
        output_var = get_operator_output_variable_name(i)
        input_variable_names[op] = output_var
        lines.append(op.to_python(output_var, input_variable_names))
    lines.append("ends = time.time()\n")
    lines.append("print('with loading: {} sec; without loading: {} sec'.format(ends-start1, ends-start2))\n")
    return prefix+'\n'+'\n'.join(lines)    
"""


import numpy as np
def evaluate_size(op, get_scaled_data=False, path=''):
    s = op.to_python('df')
    s += "\nprint('orig Nrows = {}'.format(df.shape[0]))\n"
    s += "{} = {}[{}.apply(lambda row: {}, axis=1)]".format('df1', 'df', 'df', pred_to_python_using_lambda(op.inference.input_filters[0], 'df'))
    s += "\nprint('reduced Nrows = {}'.format(df1.shape[0]))\n"
    s = s.replace("''","'")
    if get_scaled_data:
        # scale to 1GB
        s += "\nscale_factor = int(100*1000*1000/sum(df.memory_usage(index=False).tolist()))"
        # scale to 10MB
        #s += "\nscale_factor = int(10*1000*1000/sum(df.memory_usage(index=False).tolist()))"
        #s += "\nscale_factor = 50"
        s += "\nscale_df = pd.concat([df]*scale_factor)"
        s += "\nprint('scale factor = {} / {}'.format(scale_factor, scale_df.shape[0]))"
        s += "\npickle.dump(scale_df, open('{}/data/scaled_{}', 'wb'))".format(path, op.datafile_path.split('/')[-1])
    print(s)
    try:
        exec(s)
    except:
        traceback.print_exc(file=sys.stdout)
        print("FAILS")

