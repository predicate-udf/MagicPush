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

def test_1():
    schema_left = {'left_col1':'int','left_col2':'int'}
    input_table_left = generate_symbolic_table('t1',schema_left, 2)
    schema_right = {'right_col1':'int','right_col2':'int'}
    input_table_right = generate_symbolic_table('t2',schema_right, 2)
    output_filter = BinOp(Field('left_col1'),'==',Constant(1))
    input_filters = [BinOp(Field('left_col1'),'==',Constant(1)), BinOp(Field('right_col1'),'==',Constant(1))]
    op = LeftOuterJoinInference(input_table_left,input_table_right,'left_col1','right_col1',output_filter)
    op.input_filters = input_filters
    assert(op.check_small_model()==True)
    assert(op.verify_correct()==True)

def test_2():
    schema_left = {'left_col1':'int','left_col2':'int'}
    input_table_left = generate_symbolic_table('t1',schema_left, 1)
    schema_right = {'right_col1':'int','right_col2':'int'}
    input_table_right = generate_symbolic_table('t2',schema_right, 2)
    output_filter = BinOp(Field('left_col1'),'==',Constant(1))
    input_filters = [BinOp(Field('left_col1'),'==',Constant(1)), BinOp(Field('right_col1'),'==',Constant(1))]
    op = LeftOuterJoinInference(input_table_left,input_table_right,'left_col1','right_col1',output_filter)
    op.input_filters = input_filters
    assert(op.check_small_model()==True)
    assert(op.verify_correct()==True)

def test_3():
    schema_left = {'left_col1':'int','left_col2':'int'}
    input_table_left = generate_symbolic_table('t1',schema_left, 2)
    schema_right = {'right_col1':'int','right_col2':'int'}
    input_table_right = generate_symbolic_table('t2',schema_right, 2)
    output_filter = AllAnd(BinOp(Field('left_col1'),'==',Constant(1)), BinOp(Field('left_col2'),'==',Constant(2)),BinOp(Field('right_col2'),'==',Constant(2)))
    input_filters = [And(BinOp(Field('left_col1'),'==',Constant(1)), BinOp(Field('left_col2'),'==',Constant(2))), \
            And(BinOp(Field('right_col1'),'==',Constant(1)), BinOp(Field('right_col2'),'==',Constant(2)))]
    op = LeftOuterJoinInference(input_table_left,input_table_right,'left_col1','right_col1',output_filter)
    op.input_filters = input_filters
    assert(op.check_small_model()==False)
    #assert(op.verify_correct()==True)

#test_1()
#test_2()
#test_3()

def test_4():
    schema_left = {'left_col1':'int','left_col2':'int'}
    input_table_left = generate_symbolic_table('t1',schema_left, 2)
    schema_right = {'right_col1':'int','right_col2':'int'}
    input_table_right = generate_symbolic_table('t2',schema_right, 2)
    output_filter = BinOp(Field('right_col2'),'>=',Constant(2))
    input_filters = [True, BinOp(Field('right_col2'), '>=', Constant(2))]
    op = LeftOuterJoinInference(input_table_left,input_table_right,'left_col1','right_col1',output_filter)
    op.input_filters = input_filters
    assert(op.check_small_model(True))
    assert(op.verify_correct(True))
#test_4()

def test_5():
    schema_left = {'left_col1':'str','left_col2':'int'}
    input_table_left = generate_symbolic_table('t1',schema_left, 2)
    schema_right = {'right_col1':'str','right_col2':'int'}
    input_table_right = generate_symbolic_table('t2',schema_right, 2)
    output_filter = BinOp(Expr('lambda x: pd.to_datetime(x["left_col1"])','int'),'==',Constant(2))
    input_filters = [BinOp(Expr('lambda x: pd.to_datetime(x["left_col1"])','int'),'==',Constant(2)), BinOp(Expr('lambda x: pd.to_datetime(x["right_col1"])','int'),'==',Constant(2))]
    #output_filter = BinOp(Field('left_col1'), '==', Constant('1'))
    #input_filters = [BinOp(Field('left_col1'), '==', Constant('1')), BinOp(Field('right_col1'), '==', Constant('1'))]
    op = LeftOuterJoinInference(input_table_left,input_table_right,'left_col1','right_col1',output_filter)
    op.input_filters = input_filters
    assert(op.check_small_model())
    assert(op.verify_correct())
test_5()