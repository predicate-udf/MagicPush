import sys
sys.path.append("../../")
sys.path.append("../")
import z3
import dis
from test_helper import *
from pandas_op import *
from util import *
import random
from predicate import *
from util_type import *

# random filter, filter_g = filter_f & pred --> correct
def symbolic_test_1(N_tuples, N_cols):
    schema = generate_random_schema(N_cols)
    input_table = generate_symbolic_tuple('t',schema, N_tuples)
    columns = [k for k,v in schema.items()]
    op_map = {0:'==',1:'!=',2:'>',3:'<',4:'>=',5:'<='}
    cond = Constant(True)
    for i in range(random.randint(1, 2)):
        op = random.randint(0,5)
        r = random.randint(0, 100)
        random.shuffle(columns)
        field_to_compare = columns[0]
        if random.randint(0, 1) == 0:
            cond = Or(cond, BinOp(Field(field_to_compare), op_map[op], Constant(r)))
        else:
            cond = And(cond, BinOp(Field(field_to_compare), op_map[op], Constant(r)))

    condition = cond
    print("filter = {}".format(condition))
    output_filter = generate_output_filter(schema)

    op = FilterInference(input_table, condition, output_filter)
    assert(op.verify_correct())
    #op.verify_minimal()
    #return op, schema, output_filter
    
# random filter, filter_g = filter_f & pred --> correct
# allowing null values
def symbolic_test_2(N_tuples, N_cols):
    schema = generate_random_schema(N_cols)
    input_table = generate_symbolic_tuple_with_null_value('t',schema, N_tuples)
    columns = [k for k,v in schema.items()]
    op_map = {0:'==',1:'!=',2:'>',3:'<',4:'>=',5:'<='}
    cond = Constant(True)
    for i in range(random.randint(1, 2)):
        op = random.randint(0,5)
        r = random.randint(0, 100)
        random.shuffle(columns)
        field_to_compare = columns[0]
        if random.randint(0, 1) == 0:
            cond = Or(cond, BinOp(Field(field_to_compare), op_map[op], Constant(r)))
        else:
            cond = And(cond, BinOp(Field(field_to_compare), op_map[op], Constant(r)))

    condition = cond
    print("filter = {}".format(condition))
    output_filter = generate_output_filter(schema)

    op = FilterInference(input_table, condition, output_filter)
    assert(op.verify_correct())
    #op.verify_minimal()
    #return op, schema, output_filter

# dropna
def symbolic_test_3(N_tuples, N_cols):
    schema = generate_random_schema(N_cols)
    input_table = generate_symbolic_tuple_with_null_value('t',schema, N_tuples)
    columns = [k for k,v in schema.items()]
    random.shuffle(columns)
    drop_cols = columns[:2]
    output_filter = generate_output_filter(schema)

    op = DropNAInference(input_table, columns, output_filter)
    assert(op.verify_correct())
    

symbolic_test_3(2, 4)