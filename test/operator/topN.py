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
    schema = {'col1':'int','col2':'int'}
    input_table = generate_symbolic_table('t1',schema, 3)
    output_filter = BinOp(Field('col1'), '==', Constant(1))
    op = TopNInference(input_table, 2, ['col1'], True, output_filter)
    op.input_filters = [BinOp(Field('col1'), '==', Constant(1))]
    assert(op.check_small_model()==False)

def test_2():
    schema = {'col1':'int','col2':'int'}
    input_table = generate_symbolic_table('t1',schema, 3)
    output_filter = BinOp(Field('col1'), '==', Constant(1))
    op = TopNInference(input_table, 2, ['col1'], True, output_filter)
    op.input_filters = [BinOp(Field('col1'), '==', Constant(1))]
    op.input_constraint = z3.And(UniqueConstraint(['col1']).eval(input_table), FilterNonEmpty(output_filter).eval(op.run_operator([input_table])))
    assert(op.check_small_model()==True)
    assert(op.verify_correct()==True)

def test_3():
    schema = {'col1':'int','col2':'int'}
    input_table = generate_symbolic_table('t1',schema, 3)
    output_filter = BinOp(Field('col1'), '>=', Constant(1))
    op = TopNInference(input_table, 2, ['col1'], True, output_filter)
    op.input_filters = [BinOp(Field('col1'), '>=', Constant(1))]
    assert(op.check_small_model()==True)
    assert(op.verify_correct()==True)

def test_4():
    schema = {'col1':'int','col2':'int'}
    input_table = generate_symbolic_table('t1',schema, 3)
    output_filter = BinOp(Field('col2'), '==', Constant(1))
    op = TopNInference(input_table, 2, ['col1'], True, output_filter)
    op.input_filters = [BinOp(Field('col2'), '==', Constant(1))]
    #op.input_constraint = z3.And(UniqueConstraint(['col1']).eval(input_table), FilterNonEmpty(output_filter).eval(op.run_operator([input_table])))
    assert(op.check_small_model()==True)
    assert(op.verify_correct()==True)


def test_5():
    schema = {'col1':'int','col2':'int'}
    input_table = generate_symbolic_table('t1',schema, 3)
    output_filter = BinOp(Field('col2'), '>=', Constant(1))
    op = TopNInference(input_table, 2, ['col1'], True, output_filter)
    op.input_filters = [BinOp(Field('col2'), '>=', Constant(1))]
    #op.input_constraint = z3.And(UniqueConstraint(['col1']).eval(input_table), FilterNonEmpty(output_filter).eval(op.run_operator([input_table])))
    assert(op.check_small_model()==True)
    assert(op.verify_correct()==True)

#test_1()
#test_2()
#test_3()
#test_4()
test_3()