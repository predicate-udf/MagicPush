import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")

import z3
import dis
from interface import *
from util import *
import random
# from constraint import *
from predicate import *
from compare_pushdown_result import get_operator_output_variable_name
from eval_util import *
import numpy as np
import re
import functools

    
NB_path = sys.argv[1]
pipe_code, ops = read_pipeline_code(NB_path)
pipe_code.append("ops = [{}]".format(','.join(ops)))
#print(''.join(pipe_code))
exec(''.join(pipe_code))
output_schemas = generate_output_schemas(ops)
for op in ops:
    print("{} has input schema? {}".format(type(op), hasattr(op, 'input_schema')))

def generate_script_with_no_filter(ops):
    lines = []
    input_variable_names = {}
    for i,op in enumerate(ops):
        output_var = get_operator_output_variable_name(i)
        print("OP = {}".format(op))
        input_variable_names[op] = output_var
        lines.append(op.to_python(output_var, input_variable_names))
        if i == len(ops)-1:
            lines.append("print({}.head())\n".format(output_var))
    return '\n'.join(lines) 


code = generate_script_with_no_filter(ops)
print(code)
exec(code) 