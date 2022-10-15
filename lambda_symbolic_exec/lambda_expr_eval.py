import os
import sys
import tokenize
import z3
import imp
import dis
from lambda_symbolic_exec.pyvm import VirtualMachine
#sys.path.append('../')
from util_type import *
from lambda_symbolic_exec.glbs import global_uninterpreted_functions, subset_string_f

import random
import traceback
from collections import Counter
from datetime import datetime
string_candidates = ['0','','1234','something','1991-01-01']

def get_function_signature(f):
    # replace x/x['col']/... with '-', remove the effect of signature
    vars = get_lambda_variable_names(f)
    columns = get_column_used_from_lambda(f)
    s = f
    for v in vars:
        for c in columns:
            if '{}["{}"]'.format(v,c) in s:
                s = s.replace('{}["{}"]'.format(v,c), 'x')
            elif "{}['{}']".format(v,c) in s:
                s = s.replace("{}['{}']".format(v,c), 'x')
    return s[s.find(':')+1:]

def get_random_value_by_type(typ):
    return random.randint(0, 100) if typ in ['int','float','datetime'] else string_candidates[random.randint(0,4)]

def try_infer_type(func_str, vars, columns, inputs):
    new_vars = []
    var_types = []
    for i,var in enumerate(vars):
        if type(inputs[i]) is dict: # row
            temp = {}
            for c in columns:
                if c in inputs[i]:
                    var_types.append(get_variable_type(getv(inputs[i][c])))
                    temp[c] = get_random_value_by_type(var_types[-1])
            new_vars.append(temp)
        else: # single value
            var_types.append(get_variable_type(getv(inputs[i])))
            new_vars.append(get_random_value_by_type(var_types[-1]))
    try:
        #print("new vars = {}, func_str = {}".format(new_vars, func_str))
        retv = eval(func_str)(*new_vars)
        assert(retv is not None)
        return get_variable_type(retv)
    except:
        #print("var_types = {}".format(var_types))
        #traceback.print_exc(file=sys.stdout)
        c = Counter(var_types)
        return c.most_common()[0][0]
                    

def eval_uninterpreted(func_str, global_func_map, return_type, *inputs):
    vars = get_lambda_variable_names(func_str)
    columns = get_column_used_from_lambda(func_str)
    # split any row into separate variables
    var_types = []
    for i,var in enumerate(vars):
        if type(inputs[i]) is dict: # row
            for c in columns:
                if c in inputs[i]:
                    var_types.append((var, getv(inputs[i][c]), get_variable_type(getv(inputs[i][c]))))
        else: # single value
            var_types.append((var, getv(inputs[i]), get_variable_type(getv(inputs[i]))))
    sig = get_function_signature(func_str)
    if sig in global_func_map:
        newf = global_func_map[sig]
    else:
        if return_type is None:
            return_type = try_infer_type(func_str, vars, columns, inputs)
        z3_var_types = [z3.IntSort() if x[-1] in ['int','float','date','datetime'] else z3.StringSort() for x in var_types]
        z3_var_types.append(z3.IntSort() if return_type in ['int','float','date','datetime'] else z3.StringSort())
        newf = z3.Function('uninterpreted-func-{}'.format(len(global_func_map)), *z3_var_types)
        global_func_map[sig] = newf
        assert(return_type is not None)
    
    #print("{} : {}, inputs = {}".format(func_str, var_types, inputs))
    retv = newf(*[x[1] for x in var_types])
    # print("USE UNINTERPRETED FUNCTION {}, sig = {}, || {}, return {}".format(func_str, sig, newf, return_type))
    return Value(retv)

def eval_lambda(func_str, return_type, row):
    vm = VirtualMachine()
    code = eval(func_str).__code__
    main_mod = imp.new_module('__main__')
    main_mod.__builtins__ = sys.modules['builtins']
    try:
        vm.run_code(code, var_to_add={get_lambda_varibale_name(func_str):row})
        retv = vm.get_return_value()
        # print("lambda {}: ret = {}".format(func_str, retv))
        return retv
    except:
        #traceback.print_exc(file=sys.stdout)
        return eval_uninterpreted(func_str, global_uninterpreted_functions, return_type, row)
    
    

def eval_lambda_other(func_str, return_type, *inputs):
    vm = VirtualMachine()
    code = eval(func_str).__code__
    main_mod = imp.new_module('__main__')
    main_mod.__builtins__ = sys.modules['builtins']
    vars = get_lambda_variable_names(func_str)
    try:
        vm.run_code(code, var_to_add={vars[i]: inputs[i] for i in range(len(vars))})
        retv = vm.get_return_value()
        # print("lambda {}: ret = {}".format(func_str, retv))
        return retv
    except:
        #traceback.print_exc(file=sys.stdout)
        return eval_uninterpreted(func_str, global_uninterpreted_functions, return_type, *inputs)

