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

def get_operator_output_variable_name(i):
    return 'df{}'.format(i)
def generate_script_with_filter_on_input(ops, dump_path):
    lines = []
    input_variable_names = {}
    multiple_output_ops = {} # key: op, value: {nextop: var_name}
    for i,op in enumerate(ops):
        output_var = get_operator_output_variable_name(i)
        input_variable_names[op] = output_var
        if any([ix in multiple_output_ops for ix in op.dependent_ops]):
            for ix in op.dependent_ops:
                if ix in multiple_output_ops:
                    input_variable_names[ix] = multiple_output_ops[ix][op]
        lines.append(op.to_python(output_var, input_variable_names))
        if isinstance(op, InitTable):
            f = op.inference.input_filters[0]
            # YIN: added to solve memory error
            # pred_str = pred_to_python(f, output_var)
            # print(pred_str)
            # pred_str = remove_extra_brackets(pred_str)
            # print(type(pred_str))
            # exit(0)
            # end
            #lines.append("{} = {}[{}]".format(output_var, output_var, pred_to_python(f, output_var)))
            lines.append("size__before = {}.shape[0]".format(output_var))
            lines.append("{} = {}[{}.apply(lambda row: {}, axis=1)]".format(output_var, output_var, output_var, pred_to_python_using_lambda(f, output_var)))
            lines.append("size__after = {}.shape[0]".format(output_var))
            lines.append("print(\"Input {} filter reduce data from {{}} rows to {{}} rows\".format(size__before, size__after))".format(op.datafile_path))
        if type(op.inference.output_filter) is dict: # op4 -> op5(True)/op7(totalStar==1)
            # op4.output_filter = {op5: True, op7: totalStar==1}
            # op4.input_filter = True
            j = 0
            multiple_output_ops[op] = {}
            for nextop,f in op.inference.output_filter.items():
                new_var = '{}_{}'.format(output_var, j)
                j += 1
                multiple_output_ops[op][nextop] = new_var
                #lines.append("{} = {}[{}]".format(new_var, output_var, pred_to_python(f, output_var)))
                lines.append("{} = {}[{}.apply(lambda row: {}, axis=1)]".format(new_var, output_var, output_var, pred_to_python_using_lambda(f, output_var)))
                # if f == True:
                #     lines.append("{} = {}[{}]".format(new_var, output_var, pred_to_python(f, output_var)))
                # else:
                #     lines.append("{} = {}[{}]".format(new_var, output_var, pred_to_python(f, output_var)))
    if i == len(ops)-1:
        lines.append("pickle.dump({}, open('{}/result_after_pushdown.p', 'wb'))".format(output_var, dump_path))
    return '\n'.join(lines)    

def generate_script_with_filter_on_output(ops, dump_path):
    lines = []
    input_variable_names = {}
    for i,op in enumerate(ops):
        output_var = get_operator_output_variable_name(i)
        input_variable_names[op] = output_var
        lines.append(op.to_python(output_var, input_variable_names))
        if i == len(ops)-1:
            #output_filter = [v for k,v in op.inference.output_filter.items()][0]
            output_filter = op.inference.output_filter
            #lines.append("final_output = {}[{}]".format(output_var, pred_to_python(output_filter, output_var)))
            lines.append("final_output = {}[{}.apply(lambda row: {}, axis=1)]".format(output_var, output_var, pred_to_python_using_lambda(output_filter, output_var)))
            lines.append("pickle.dump(final_output, open('{}/result_pred_on_output.p', 'wb'))".format(dump_path))
    return '\n'.join(lines)    

def generate_script_with_no_filter(ops, dump_path):
    lines = []
    input_variable_names = {}
    for i,op in enumerate(ops):
        output_var = get_operator_output_variable_name(i)
        input_variable_names[op] = output_var
        lines.append(op.to_python(output_var, input_variable_names))
        if i == len(ops)-1:
            lines.append("pickle.dump({}, open('{}/result_original.p', 'wb'))".format(output_var, dump_path))
    return '\n'.join(lines)    

prefix = """
import pandas as pd
import pickle
"""

def compare_result(dump_path):
    output1 = pickle.load(open(dump_path+"/result_pred_on_output.p", 'rb'))
    output2 = pickle.load(open(dump_path+"/result_after_pushdown.p", 'rb'))
    columns = [c for c in output1.columns]
    output1 = output1[sorted(output1.columns)]
    output2 = output1[sorted(output2.columns)]
    print(output1.head())
    print(output2.head())
    try:
        pd.testing.assert_frame_equal(output1.reset_index(drop=True).sort_values(columns), output2.reset_index(drop=True).sort_values(columns), check_dtype=False)
        print("RESULT EQUIVALENT, YAY!!")
    except Exception as e:
        print("RESULT NOT EQUIVALENT, CHECK ERROR DETAILS:")
        print(e)

def get_output_filter(ops, dump_path):
    code = generate_script_with_no_filter(ops, dump_path)
    print(code)
    exec(code)
    output = pickle.load(open(dump_path+"/result_original.p", 'rb'))
    # print(output)
    idx = random.randint(0, output.shape[0]-1)
    row = output.iloc[idx]
    pred = []
    for col in output.columns:
        op = '=='
        # Yin: There are duplicated cols
        # if not hasattr(output[col], 'dtype'):
        #     temp = output[col].drop_duplicates().T.drop_duplicates().T
        #     typ = type(temp.iloc[0][0])
        #     pred.append(BinOp(Field(str(col)), op, Constant(row[col], typ=str(typ).replace("dtype(",'').replace("'",'').replace(')',''))))
        # else:
        if col not in ['index_x','index_y','index']:
            # print(output[col])
            # if str(output[col].dtype) == 'datetime64[ns]': 
            #     type = 'datetime'
            #     pred.append(BinOp(Field(str(col)), op, Constant(row[col], typ=type.replace("dtype(",'').replace("'",'').replace(')',''))))
            if pd.isnull(row[col]):
                pred.append(IsNULL(Field(str(col))))
            else:
                pred.append(BinOp(Field(str(col)), op, Constant(row[col], typ=str(output[col].dtype).replace("dtype(",'').replace("'",'').replace(')',''))))
    # pred.append(BinOp(Field('index'), '==', Constant('3.png', typ = 'str')))
    #index_name = output.index.name
    #if index_name is not None:
    #    pred.append(BinOp(Field(str(index_name)), op, Constant(output.index[idx], typ=str(output.index.dtype))))
    # for pred_ in pred:
    #     # print(pred_)
    return AllAnd(*pred)

def get_output_filter_all_operators(ops, dump_path):
    code = generate_script_with_no_filter(ops, dump_path)
    # print(code)
    exec(code)
    output = pickle.load(open(dump_path+"/result_original.p", 'rb'))
    idx = random.randint(0, output.shape[0]) -1
    row = output.iloc[idx]
    pred = []
    cnt = 0 
    for col in output.columns:
        #print(output[col])
        if str(output[col].dtype) == 'float64' or str(output[col].dtype) == 'int64':
            op = generate_op()
        else:
            op = '=='
        pred.append(BinOp(Field(col), op, Constant(row[col], typ=str(output[col].dtype).replace("dtype(",'').replace("'",'').replace(')',''))))
        cnt += 1
        if (cnt>5):
           break
    index_name = output.index.name
    if index_name is not None:
        if str(output[col].dtype) == 'float64' or str(output[col].dtype) == 'int64':
            op = generate_op()
        else:
            op = '=='
        pred.append(BinOp(Field(index_name), op, Constant(output.index[idx], typ=str(output.index.dtype))))
    for pred_ in pred:
        print(pred_)
    result = generate_pred(pred)
    return result



def generate_pred(pred):
    if len(pred) == 1:
        return pred[0]
    else:
        flag = geneate_bool()
        if flag:
            return And(pred[0], generate_pred(pred[1:]))
        else:
            return Or(pred[0], generate_pred(pred[1:]))

def generate_op():
    op_set = ['==', '>', '<', '>=', '<=', '!=']
    return choice(op_set)

def geneate_bool():
    op_set = [0,1]
    return choice(op_set)

# def remove_extra_brackets(pred_str):
#     result = []
#     str_lst = pred_str.split('&')
#     for str_i in str_lst:
#         str_i = check_match(str_i)
#         result.append(str_i)
#     result_str = result[0]
#     for r in result[1:]:
#         result_str + " & " + r
#     #print(result_str)
#     return result_str
    
# def check_match(str_i):
#     stack = []
#     tail = 0
#     for i in str_i:
#         if i == '(':
#             stack.append(i)
#         if i == ')':
#             if(len(stack) == 0):
#                 tail +=1 
#                 continue
#             else:
#                 stack.pop()
#     head = len(stack)
#     tail = len(str_i)  - tail
#     return str_i[head:tail]

from io import StringIO
import contextlib

@contextlib.contextmanager
def stdoutIO(stdout=None):
    old = sys.stdout
    if stdout is None:
        stdout = StringIO()
    sys.stdout = stdout
    yield stdout
    sys.stdout = old


def check_pushdown_result(ops, temp_dump_path):
    code1 = generate_script_with_filter_on_output(ops, temp_dump_path)
    code2 = generate_script_with_filter_on_input(ops, temp_dump_path)

    print("======")
    print(code1)
    print("======")
    print(code2)
    print("======")
    
    with stdoutIO() as s:
        exec(code1)
        exec(code2)
    print(s.getvalue())

    compare_result(temp_dump_path)
    