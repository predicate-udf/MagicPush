from ast import IsNot
from lib2to3.pytree import convert
import sys
sys.path.append("../../")
sys.path.append("../")
import z3
import dis
from test_helper import *
from util_type import *
from pandas_op import *
from util import *
import random
from table_constraint import *
from predicate import *

def prepare(N_tuples, N_cols):
    schema = {'col_g':'int','col_agg':'int'} #generate_random_schema(N_cols)
    input_table = generate_symbolic_table('t',schema, N_tuples)
    cols = [k for k,v in schema.items()]
    random.shuffle(cols)
    groupby_cols = ['col_g']#cols[:2]
    aggr_cols = ['col_agg']#cols[2:random.randint(3, N_cols-1)]
    print("schema = {}, groupby_cols = {}, aggr_cols = {}".format(schema, groupby_cols, aggr_cols))
    
    output_schema = {col:schema[col] for col in groupby_cols}
    new_column_names = {col: 'agg_{}'.format(col) for col in aggr_cols}
    output_schema.update({new_column_names[col]:schema[col] for col in aggr_cols})
    return schema, input_table, groupby_cols, aggr_cols, new_column_names, output_schema

# case 1: groupby.sum, filter_f = (col_g==p1 && col_sum==p2) --> incorrect, need to rewrite to the form in test 3
def symbolic_test_1(N_tuples, N_cols):
    schema, input_table, groupby_cols, aggr_cols, new_column_names, output_schema = prepare(N_tuples, N_cols)

    aggr_func_map = {col:(Value(0,True), 'sum') for col in aggr_cols}
    output_filter = generate_output_filter(output_schema)
    print("output filter = {}".format(output_filter))
    op = GroupByInference(input_table, groupby_cols, aggr_func_map, new_column_names, output_filter)
    assert(op.check_small_model()==False)

# case 2: groupby.sum, filter_f = (col_sum>100) --> not SM, incorrect
def symbolic_test_2(N_tuples, N_cols):
    schema, input_table, groupby_cols, aggr_cols, new_column_names, output_schema = prepare(N_tuples, N_cols)

    aggr_func_map = {col:(Value(0,True), 'sum') for col in aggr_cols}
    output_filter = AllAnd(*[BinOp(Field(v), '>', Constant(100)) for k,v in new_column_names.items()])
    op = GroupByInference(input_table, groupby_cols, aggr_func_map, new_column_names, output_filter)

    assert(op.check_small_model()==False)
    
# case 3: groupby.sum, filter_f = (col_g==p) --> SM, correct
def symbolic_test_3(N_tuples, N_cols):
    schema, input_table, groupby_cols, aggr_cols, new_column_names, output_schema = prepare(N_tuples, N_cols)

    aggr_func_map = {col:(Value(0,True), 'sum') for col in aggr_cols}
    output_filter = AllAnd(*[BinOp(Field(col), '==', Variable(get_symbolic_value_by_type(schema[col], "output_col_{}".format(col)), schema[col])) for col in groupby_cols])
    op = GroupByInference(input_table, groupby_cols, aggr_func_map, new_column_names, output_filter)

    assert(op.check_small_model())
    assert(op.verify_correct())

# case 4: groupby.min, filter_f = (col_min<100) --> SM, correct
def symbolic_test_4(N_tuples, N_cols):
    schema, input_table, groupby_cols, aggr_cols, new_column_names, output_schema = prepare(N_tuples, N_cols)

    # max_of_all = {k:input_table[0][k] for k in aggr_cols}
    # for t in input_table[1:]:
    #     for k,v in max_of_all.items():
    #         max_of_all[k] = z3.If(v>t[k], v, t[k])
    # aggr_func_map = {col:(max_of_all[col]+1, lambda x, y: z3.If(x<y, x, y)) for col in aggr_cols} # min
    aggr_func_map = {col:(Value(0,True), 'lambda x, y: x if x<y else y') for col in aggr_cols} # min
    output_filter = AllAnd(*[BinOp(Field(v), '<', Constant(100)) for k,v in new_column_names.items()])
    op = GroupByInference(input_table, groupby_cols, aggr_func_map, new_column_names, output_filter)
    op.input_filters = [AllAnd(*[BinOp(Field(col), '<', Constant(100)) for col in aggr_cols])]

    assert(op.check_small_model())
    assert(op.verify_correct())

# case 5: groupby.min, filter_f = (col_min>100) --> not SM, incorrect
def symbolic_test_5(N_tuples, N_cols):
    schema, input_table, groupby_cols, aggr_cols, new_column_names, output_schema = prepare(N_tuples, N_cols)
    
    # max_of_all = {k:input_table[0][k] for k in aggr_cols}
    # for t in input_table[1:]:
    #     for k,v in max_of_all.items():
    #         max_of_all[k] = z3.If(v>t[k], v, t[k])
    aggr_func_map = {col:(Value(0, True), 'lambda x, y: x if x<y else y') for col in aggr_cols} # min
    output_filter = AllAnd(*[BinOp(Field(v), '>', Constant(100)) for k,v in new_column_names.items()])
    op = GroupByInference(input_table, groupby_cols, aggr_func_map, new_column_names, output_filter)
    op.input_filters = [AllAnd(*[BinOp(Field(col), '>', Constant(100)) for col in aggr_cols])]

    #assert(op.check_small_model()==False)
    assert(op.verify_correct()==False)

#symbolic_test_5(2, 2)

# case 6: group exists a value k, return v, else return z --> SM, correct
def symbolic_test_6(N_tuples, N_cols):
    schema, input_table, groupby_cols, aggr_cols, new_column_names, output_schema = prepare(N_tuples, N_cols)

    k = 1 #z3.Int('k')
    v = 2 #z3.Int('v')
    z = 3 #z3.Int('z')
    # v if y==k else x
    aggr_func_map = {col: (Value(z), 'lambda x,y: 2 if y==1 else x') for col in aggr_cols} # x: prev value, y: new element
    output_filter = AllAnd(*[BinOp(Field(v_), '==', Variable(v)) for k_,v_ in new_column_names.items()])
    op = GroupByInference(input_table, groupby_cols, aggr_func_map, new_column_names, output_filter)
    op.input_filters = [AllAnd(*[BinOp(Field(col), '==', Variable(k)) for col in aggr_cols])]

    assert(op.check_small_model())
    assert(op.verify_correct())


# # case 7: drop_duplicates, filter_f = (col_g==p1 && col_drop==p2) --> not SM
def symbolic_test_7(N_tuples, N_cols):
    schema, input_table, groupby_cols, aggr_cols, new_column_names, output_schema = prepare(N_tuples, N_cols)
    
    output_filter = AllAnd(*[BinOp(Field(k), '==', Constant(random.randint(0, 1000))) for k,v in schema.items()])
    op = DropDuplicateInference(input_table, groupby_cols, output_filter)

    assert(op.check_small_model()==False)
    

# # case 8: drop_duplicates, filter_f = (col_g==p1) --> SM, correct
def symbolic_test_8(N_tuples, N_cols):
    schema, input_table, groupby_cols, aggr_cols, new_column_names, output_schema = prepare(N_tuples, N_cols)
    
    output_filter = AllAnd(*[BinOp(Field(k), '==', Constant(random.randint(0, 1000))) for k in groupby_cols])
    op = DropDuplicateInference(input_table, groupby_cols, output_filter)

    assert(op.check_small_model())
    assert(op.verify_correct())

#symbolic_test_8(2, 3)

# # case 8_2: drop_duplicates, filter_f = (col_g==p1 && col_drop==p2) and some record passes output filter
def symbolic_test_8_2(N_tuples, N_cols):
    schema, input_table, groupby_cols, aggr_cols, new_column_names, output_schema = prepare(N_tuples, N_cols)
    
    random_filter_value = {k:random.randint(0, 1000) for k,v in schema.items()}
    output_filter = AllAnd(*[BinOp(Field(k), '==', Constant(random_filter_value[k])) for k,v in schema.items()])
    op = DropDuplicateInference(input_table, groupby_cols, output_filter)
    # under the condition that output filter does not return empty result
    #op.input_constraint = input_table[1]['col_agg'].v==random_filter_value['col_agg']
    rs1_temp = op.run_operator(op.input_tables)
    rs1 = op.run_output_filter(rs1_temp)
    op.input_constraint = rs1[0].eval_exist_cond()==True

    assert(op.check_small_model())
    assert(op.verify_correct())


# case 9: AllAggr, to-fix
def symbolic_test_9():
    #schema, input_table, groupby_cols, aggr_cols, new_column_names, output_schema = prepare(N_tuples, N_cols)
    input_schema = {'nation':'int','id':'int','volumn':'int'}
    input_table = generate_symbolic_table('t', input_schema, 2)
    output_filter = True
    op = AllAggrInference(input_table, Value(0), "lambda x, y: x+y['volumn'] if y['nation']==1 else x", output_filter)
    op.input_filters = [BinOp(Field('nation'), '==', Constant(1))]

    #op.input_constraint = z3.Or(input_table[0]['nation'].v==1, input_table[1]['nation'].v==1)
    assert(op.check_small_model())
    assert(op.verify_correct()) 
    # TODO: when no tup satisfies If, F(Op(T))=0 while Op(G(T))=null; only correct when adding constraint that at least some tuple matches If 

# def all_aggr_test():
# AllAggrInference(OperatorInference):
#     def __init__(self, input_table, initv, aggr_func, output_filter,

#symbolic_test_9()

# all aggr, take the first/last value, eq to "df[0]"
# if output exists, then it is correct.
def symbolic_test_10():
    input_schema = {'col1':'int','col2':'int'}
    input_table = generate_symbolic_table('t', input_schema, 2)
    output_filter = BinOp(Field('output_col'),'==',Constant(3))
    op = AllAggrInference(input_table, Value(0,True), "lambda x,y: y['col1']",output_filter, convert_to_tuple=lambda x:{'output_col':x})
    op.input_filters = [BinOp(Field('col1'), '==', Constant(3))]
    op.output_exists()
    assert(op.check_small_model())
    assert(op.verify_correct())

#symbolic_test_10()

def symbolic_test_11():
    input_schema = {'col1':'int','col2':'int','col3':'int'}
    input_table = generate_symbolic_table('t', input_schema, 2)
    
    output_filter = AllAnd(*[Not(IsNULL(Field('col1'))),Not(IsNULL(Field('col2'))), BinOp(Field('col3'), '>', Constant(100))])
    op = DropDuplicateInference(input_table, ['col1','col2'], output_filter)
    op.input_filters = [And(Not(IsNULL(Field('col1'))),Not(IsNULL(Field('col2'))))]
    assert(op.check_small_model(True))
    assert(op.verify_correct(True))
symbolic_test_11()