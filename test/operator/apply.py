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

# simple case: apply(col['x'] + 1)
def symbolic_test_1(N_tuples, N_cols):
    schema = generate_random_schema(N_cols-1, required_columns={'colx':'int'})
    input_table = generate_symbolic_table('t',schema, N_tuples)
    columns = [k for k,v in schema.items()]
    f = lambda x: x['colx'].v + 1
    new_col_name = 'new_v'
    output_filter = And(BinOp(Field('col_a'), '==', Constant(random.randint(0, 100))), \
                        BinOp(Field('new_v'), '==', Constant(random.randint(0, 100))))
    op = SetItemInference(input_table, new_col_name, f, output_filter)
    
    assert(op.verify_correct())
    
# lambda x: |x|
def symbolic_test_2(N_tuples, N_cols):
    schema = generate_random_schema(N_cols-1, required_columns={'colx':'int'})
    input_table = generate_symbolic_tuple('t',schema, N_tuples)
    columns = [k for k,v in schema.items()]
    f = lambda x: z3.If(x['colx'].v>0, x['colx'].v, 0-x['colx'].v)
    new_col_name = 'new_v'
    output_filter = And(BinOp(Field('col_a'), '==', Constant(random.randint(0, 100))), \
                        BinOp(Field('new_v'), '==', Constant(random.randint(0, 100))))
    op = SetItemInference(input_table, new_col_name, f, output_filter)
    
    assert(op.verify_correct())
  
# a fun case:  
# lambda x: |x|
# output filter: [new_v] == 10
# input filter: colx==10 || colx==-10
def symbolic_test_3(N_tuples, N_cols):
    schema = generate_random_schema(N_cols-1, required_columns={'colx':'int'})
    input_table = generate_symbolic_tuple('t',schema, N_tuples)
    columns = [k for k,v in schema.items()]
    f = lambda x: z3.If(x['colx'].v>0, x['colx'].v, 0-x['colx'].v)
    new_col_name = 'new_v'
    output_filter = BinOp(Field('new_v'), '==', Constant(10))
    input_filter = Or(BinOp(Field('colx'), '==', Constant(10)), \
                        BinOp(Field('colx'), '==', Constant(-10)))
    op = SetItemInference(input_table, new_col_name, f, output_filter)
    op.input_filters = [input_filter]
    
    assert(op.verify_correct())
    
    
# fillna
def symbolic_test_4(N_tuples, N_cols):
    schema = generate_random_schema(N_cols-1, required_columns={'colx':'int'})
    input_table = generate_symbolic_tuple_with_null_value('t',schema, N_tuples)
    columns = [k for k,v in schema.items()]
    col = 'colx'
    output_filter = And(BinOp(Field('col_a'), '==', Constant(random.randint(0, 100))), \
                        BinOp(Field('colx'), '==', Constant(random.randint(0, 100))))
    op = FillNAInference(input_table, col, 0, output_filter)
    
    assert(op.verify_correct())
    
    
# fillna
def symbolic_test_5(N_tuples, N_cols):
    schema = generate_random_schema(N_cols-1, required_columns={'colx':'int'})
    input_table = generate_symbolic_tuple_with_null_value('t',schema, N_tuples)
    columns = [k for k,v in schema.items()]
    col = 'colx'
    filterv = random.randint(0, 100)
    output_filter = And(BinOp(Field('col_a'), '==', Constant(filterv)), \
                        BinOp(Field('colx'), '==', Constant(0)))
    op = FillNAInference(input_table, col, 0, output_filter)
    op.input_filters = [And(BinOp(Field('col_a'), '==', Constant(filterv)), \
        Or(BinOp(Field('colx'), '==', Constant(0)), IsNULL(Field('colx'))))]
    
    assert(op.verify_correct())
    
# fillna
def symbolic_test_6(N_tuples, N_cols):
    schema = generate_random_schema(N_cols-1, required_columns={'colx':'int'})
    input_table = generate_symbolic_tuple_with_null_value('t',schema, N_tuples)
    columns = [k for k,v in schema.items()]
    col = 'colx'
    filterv = random.randint(0, 100)
    output_filter = And(BinOp(Field('col_a'), '==', Constant(filterv)), \
                        BinOp(Field('colx'), '==', Constant(0)))
    op = FillNAInference(input_table, col, 0, output_filter)
    op.input_filters = [And(BinOp(Field('col_a'), '==', Constant(filterv)), \
        BinOp(Field('colx'), '==', Constant(0)))]
    
    assert(op.verify_correct()==False)
    
symbolic_test_6(2, 3)