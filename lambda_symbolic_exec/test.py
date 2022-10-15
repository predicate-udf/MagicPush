import os
import sys
import tokenize
import z3
import imp
import dis
from pyvm import VirtualMachine
sys.path.append('../')
from lambda_symbolic_exec.lambda_expr_eval import *


func_str = "lambda x, row: {{k:(row['head'] if k == row['value'] else v) for k,v in x.items()}}"
eval_lambda_other(func_str, {1:Value(0,True),2:Value(0,True)}, {'head':z3.Int('v-h'), 'value':z3.Int('v-v')})
"""
def get_lambda_varibale_name(expr):
    expr_str = expr
    var = expr_str[expr_str.find('lambda ')+len('lambda '):expr_str.find(':')]
    return var

def get_row(expr, str_type=[]):
    var = get_lambda_varibale_name(expr)
    print(var)
    instrs = [instr for instr in dis.Bytecode(eval(expr).__code__)]
    cols = []
    for i,instr in enumerate(instrs):
        if str(instr.opname) in ["LOAD_FAST",'LOAD_GLOBAL'] and str(instr.argval) == var and i < len(instrs)-1 and instrs[i+1].opname == "LOAD_CONST":
            cols.append(instrs[i+1].argval)
    return {col:Value(z3.Int('v-'+col), z3.Bool('v-'+col+'-null')) if col not in str_type else z3.String('v-'+col) for col in set(cols)}

def print_code(f):
    instrs = [instr for instr in dis.Bytecode(f)]
    for i,instr in enumerate(instrs):
       print(instr)

vm = VirtualMachine()

#fstr = "lambda x: x['a'] + x['b']"
#fstr = "lambda xxx__: 1 if not pd.isnull(xxx__['click_date']) else 0"
#fstr = 'lambda xxx___: { "not_recom":"0" }[xxx___["parents_great_pret"]] if xxx___["parents_great_pret"] in ["not_recom"] else  xxx___["parents_great_pret"]'
#fstr = 'lambda xxx__: "Unknown" if xxx__["Income Level"] == np.nan else xxx__["Income Level"]'
fstr = 'lambda xxx__: str(xxx__["Cumulative_Cases"]).replace(",","")'

row = get_row(fstr,['Cumulative_Cases'])
print(row)
code = eval(fstr).__code__
print_code(code)
print(get_lambda_varibale_name(fstr))

main_mod = imp.new_module('__main__')
main_mod.__builtins__ = sys.modules['builtins']

print( main_mod.__dict__)
vm.run_code(code, var_to_add={get_lambda_varibale_name(fstr):row})
"""