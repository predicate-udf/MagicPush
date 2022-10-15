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


# filter with a conjunction of left/right column : correct
def symbolic_test_1(N_tuples):
    N_cols = 2
    schema_left = generate_random_schema(N_cols)
    input_table_left = generate_symbolic_table('t',schema_left, N_tuples)
    columns_left = [k for k,v in schema_left.items()]
    schema_right = generate_random_schema(N_cols, N_cols+1)
    schema_right[columns_left[-1]] = schema_left[columns_left[-1]] # merge column
    input_table_right = generate_symbolic_table('t',schema_right, N_tuples)
    columns_right = [k for k,v in schema_right.items()]

    merge_col = columns_left[-1]
    
    eq_values = {col:random.randint(0,1000) for col in columns_left}
    eq_values.update({col:random.randint(0,1000) for col in columns_right})
    output_filter = AllAnd(*[BinOp(Field(col), '==', Constant(v)) for col,v in eq_values.items()])

    #print("output filter = {}".format(output_filter))
    join_op = InnerJoiInferencen(input_table_left, input_table_right, merge_col, output_filter)

    assert(join_op.verify_correct())
#symbolic_test_1(2)

# filter with including a comparison between left and right : not correct
def symbolic_test_2(N_tuples):
    N_cols = 2
    schema_left = generate_random_schema(N_cols)
    input_table_left = generate_symbolic_table('t',schema_left, N_tuples)
    columns_left = [k for k,v in schema_left.items()]
    schema_right = generate_random_schema(N_cols, N_cols+1)
    schema_right[columns_left[-1]] = schema_left[columns_left[-1]] # merge column
    input_table_right = generate_symbolic_table('t',schema_right, N_tuples)
    columns_right = [k for k,v in schema_right.items()]

    merge_col = columns_left[-1]
    
    output_filter = And(BinOp(Field(columns_left[0]), '==', Constant(random.randint(0, 1000))), \
                        BinOp(Field(columns_left[0]), '>', Field(columns_right[0])))

    print("output filter = {}".format(output_filter))
    join_op = InnerJoinInference(input_table_left, input_table_right, merge_col, output_filter)

    assert(join_op.verify_correct() == False)
    
    
# filter with a conjunction of left/right column : correct
# testing null value
def symbolic_test_3(N_tuples):
    N_cols = 2
    schema_left = generate_random_schema(N_cols)
    input_table_left = generate_symbolic_tuple_with_null_value('t',schema_left, N_tuples)
    columns_left = [k for k,v in schema_left.items()]
    schema_right = generate_random_schema(N_cols, N_cols+1)
    schema_right[columns_left[-1]] = schema_left[columns_left[-1]] # merge column
    input_table_right = generate_symbolic_tuple_with_null_value('t',schema_right, N_tuples)
    columns_right = [k for k,v in schema_right.items()]

    merge_col = columns_left[-1]
    
    eq_values = {col:random.randint(0,1000) for col in columns_left}
    eq_values.update({col:random.randint(0,1000) for col in columns_right})
    output_filter = AllAnd(*[BinOp(Field(col), '==', Constant(v)) for col,v in eq_values.items()])

    #print("output filter = {}".format(output_filter))
    join_op = InnerJoinInference(input_table_left, input_table_right, merge_col, output_filter)

    assert(join_op.verify_correct())
    
symbolic_test_3(2)

