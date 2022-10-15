import sys
sys.path.append("../../")
sys.path.append("../")
import z3
import dis
from test_helper import *
from pandas_op import *
from util import *
import random
from table_constraint import *
from predicate import *


# TODO: pivot need to add input constraint to say that output value exists

# FOR PIVOT: need to rewrite output predicate to --> index==v

def prepare():
    output_schema = {100:'int',101:'int',102:'int'}
    columns = ["col_index",'colheader','col_value']
    schema = {k:'int' for k in columns}
    input_table = generate_symbolic_table('t', schema, len(output_schema))
    index_col = columns[0]
    header_col = columns[1]
    value_col = columns[2]
    
    return input_table, index_col, header_col, value_col, output_schema

# output filter = And(index_col == v1, header1 == v2, header2 == v3, ...) --> FM, correct
# aggr_func = None, input_constraint is [index_col, header_col] is unique
# trivially holds because N_tuples has more values than header values, adding same_index assumption, pre-condition does not hold
def symbolic_test_1():
    input_table, index_col, header_col, value_col, output_schema = prepare()
    
    
    output_filter = AllAnd(*[BinOp(Field(col), '==', Constant(random.randint(0, 10))) for col,typ in output_schema.items()])
    
    print("output filter = {}".format(output_filter))
    op = PivotInference(input_table, index_col, header_col, value_col, None, output_filter, output_schema)
    
    assert(op.check_small_model())
    assert(op.verify_correct())

symbolic_test_1()
    
# output filter = And(index_col == v1, header1 == v2, header2 == v3, ...) --> FM, incorrect
# aggr_func = None, input_constraint is [index_col, header_col] is unique
# N_tuples is the same as header values
# example:
# if we have the following pivot result schema
# index_col   2    5
#     1       3    100
# filter_f = [index_col]==1 && [2]==3 && [5]==100
# filter_g = [index_col]==1 && (([header_col]==2 && [value_col]==3) || ([header_col]==5 && [value_col]==100))

# assuming we have input table:
# index_col   header_col   value_col
#     1           2           3
#     1           5           6
# f(pivot(T)) = \emptyset
# pivot(g(T)) = 1 3 null
# these two are not equal!
# This is because weather a tuple exist or not depends on other rows with the same index
def symbolic_test_2():
    N_cols = 3
    output_schema_sz = 2
    N_tuples = output_schema_sz
    input_table, index_col, header_col, value_col, output_schema = prepare(N_cols, output_schema_sz, N_tuples)
    
    input_constraint = []
    for i in range(len(input_table)):
        for j in range(i+1, len(input_table)):
            input_constraint.append(z3.Or(input_table[i][index_col].v!=input_table[j][index_col].v, \
                                          input_table[i][header_col].v!=input_table[j][header_col].v))
    input_constraint = z3.And(*input_constraint)
    
    output_filter = AllAnd(*[BinOp(Field(col), '==', Constant(random.randint(0, 1000))) for col,typ in output_schema.items()])
    
    print("output filter = {}".format(output_filter))
    op = Pivot(input_table, index_col, header_col, value_col, None, output_filter, output_schema)
    op.add_input_constraint(input_constraint)
    
    assert(op.check_small_model())
    assert(op.verify_correct()==False)
    
# output filter = And(index_col == v1, header1 == v2, header2 == v3, ...) --> FM, incorrect
# aggr_func = take last value, no unique constraint
def symbolic_test_3():
    N_cols = 3
    output_schema_sz = 2
    N_tuples = output_schema_sz + 2 
    input_table, index_col, header_col, value_col, output_schema = prepare(N_cols, output_schema_sz, N_tuples)
    
    output_filter = AllAnd(*[BinOp(Field(col), '==', Constant(random.randint(0, 1000))) for col,typ in output_schema.items()])
    
    print("output filter = {}".format(output_filter))
    op = PivotInference(input_table, index_col, header_col, value_col, None, output_filter, output_schema)
    
    assert(op.check_small_model())
    assert(op.verify_correct()==False)
    

# output filter = And(index_col == v1) --> FM, correct
# aggr_func = sum
# input_filter set to And(index_col == v1)
def symbolic_test_4():
    N_cols = 3
    output_schema_sz = 2
    N_tuples = output_schema_sz + 2
    input_table, index_col, header_col, value_col, output_schema = prepare(N_cols, output_schema_sz, N_tuples)
    output_filter = BinOp(Field(index_col), '==', Constant(random.randint(0, 1000)))
    #print("output filter = {}".format(output_filter))
    aggr_func = (Value(0, True), lambda x, y: x+y)
    op = PivotInference(input_table, index_col, header_col, value_col, aggr_func, output_filter, output_schema)
        
    assert(op.check_small_model())
    assert(op.verify_correct())
    