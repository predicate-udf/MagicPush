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
from interface import *
from table_constraint import *


def prepare(columns):
    schema = {k:'int' for k in columns} #generate_random_schema(N_cols)
    input_table = generate_symbolic_table('t2',schema, 2)
    return schema, input_table

def get_previous_op(ops, op):
    res = []
    for o in ops:
        if op in o.dependent_ops:
            res.append(o)
    return res

def generate_output_filter_from_previous(op, ops):
    previous_ops = get_previous_op(ops, op)
    if len(previous_ops) == 0: # the last operator in the data pipeline.
        return {None:None}
    else:
        output_filters = {}
        for p_op in previous_ops:
            idx = p_op.dependent_ops.index(op)
            output_filters[p_op] = p_op.inference.input_filters[idx]
        return output_filters


def get_inference(input_table, path, group_cols, input_filters, output_filter, output_exist=False):
    
    pipe_full_len = len(path.operators) if type(path.cond) is bool else len(path.operators)+1
    sub_pipeline = [None for i in range(pipe_full_len)]
    sub_pipeline_dependency = [[] for i in range(pipe_full_len)]
    op_id_map = {op:i for i,op in enumerate(path.operators)}
    for i,op in reversed(list(enumerate(path.operators+([] if type(path.cond) is bool else [path.cond])))):
        if(op == path.operators[-1]):
            output_filter_i = {None:output_filter}
        else:
            output_filter_i = generate_output_filter_from_previous(op, path.operators)
        output_filter_i = AllOr(*list(output_filter_i.values()))
        inference = op.get_inference_instance(output_filter_i)
        if isinstance(op, SubpipeInput):
            op.inference.input_tables = [input_table]
        # TODO for yin: get input filter for this subpipeline operator
        inference.input_filters = input_filters[op] if type(input_filters[op]) is list else [input_filters[op]] 
        
        sub_pipeline[i] = inference
        sub_pipeline_dependency[i] = [op_id_map[op_] if op_ in op_id_map else [] for op_ in op.dependent_ops]
    
    path_inf = GroupedMapOnePathInference(input_table, group_cols, sub_pipeline, sub_pipeline_dependency, \
            True if type(path.cond) is bool else sub_pipeline[-1], output_filter)
    if output_exist:
        path_inf.output_exists()
    return path_inf

"""
def normalize(group):
    x = group[group.Time==2007].Value.values[0]
    df = group.copy()
    df['Value'] = df.Value/x
    return df[['Time','Value']]
wage_index = real_wage.groupby('Country').apply(normalize).reset_index().drop('level_1',axis=1)
"""
def test_1():
    schema, input_table = prepare(['Country', 'Time', 'Value'])
    sub_op0 = SubpipeInput(None, 'group', ['Country'])
    sub_op0.input_schema = schema
    sub_op1 = Filter(sub_op0, BinOp(Field('Time'), '==', Constant(2007)))
    x = AllAggregate(sub_op1, Value(0), "lambda x,y: y['Value']")
    sub_op2 = SetItemWithDependency(sub_op0, 'Value', {'df':sub_op0, 'x':x}, 'lambda df,x: df["Value"]/x')
    wage_index = CogroupedMap(SubPipeline(PipelinePath([sub_op0, sub_op1, x, sub_op2])))

    # output_filter = AllAnd(*[BinOp(Field('Country'), '==', Constant(1)), BinOp(Field('Time'),'==',Constant(2000)), BinOp(Field('Value'),'==',Constant(10))])
    # input_table_filter = Or(BinOp(Field('Time'), '==', Constant(2000)), BinOp(Field('Time'), '==', Constant(2007)))
    # input_filters = {sub_op2:[BinOp(Field('Time'), '==', Constant(2000)),True], \
    #         x:BinOp(Field('Time'), '==', Constant(2000)), \
    #         sub_op1:BinOp(Field('Time'), '==', Constant(2007)), \
    #         sub_op0:input_table_filter}
    output_filter = BinOp(Field("Time"), ">=", Constant(2007))
    input_filters = {sub_op2: [BinOp(Field("Time"), ">=", Constant(2007)),True],\
        x:True, sub_op1:BinOp(Field("Time"), ">=", Constant(2007)),\
            sub_op0: BinOp(Field("Time"), ">=", Constant(2007))  }
    
    inf = get_inference(input_table, wage_index.subpipeline.paths[0], sub_op0.group_key, input_filters, output_filter)
    assert(inf.verify_correct())
    

# df_oecd_normalized = df_oecd.groupby('Country Name').apply(normalize_to_2000)
# def normalize_to_2000(df):
#     ref = df.loc[df.year == 2000].iloc[0]['GDPPC']
#     df.GDPPC /= ref  
#     df.GDPPC = (df.GDPPC * 100) - 100
#     return df
def test_2():
    schema, input_table = prepare(['Country Name', 'GDPPC', 'Country Code', 'year'])
    sub_op0_1 = SubpipeInput(None, 'group', ['Country Name'])
    sub_op0_1.input_schema = schema
    sub_op1_1 = Filter(sub_op0_1, BinOp(Field('year'), '==', Constant(2000)))
    sub_op2_1 = AllAggregate(sub_op1_1, Value(0, True), "lambda x,y: y['GDPPC']")
    sub_op3_1 = SetItemWithDependency(sub_op0_1, 'GDPPC', {'x':sub_op0_1, 'ref':sub_op2_1}, 'lambda x,ref:(x["GDPPC"]/ref)*100-100')
    df_oecd_normalized = CogroupedMap(SubPipeline([PipelinePath([sub_op0_1, sub_op1_1, sub_op2_1, sub_op3_1])]))
    

    output_filter = And(BinOp(Field('year'), '>=', Constant(2000)), BinOp(Field('Country Code'), '==', Constant(231)))
    input_filters = {sub_op3_1: [output_filter,True], sub_op2_1: True, sub_op1_1:BinOp(Field('year'), '==', Constant(2000)),\
    sub_op0_1: Or(And(BinOp(Field('year'), '>=', Constant(2000)), BinOp(Field('Country Code'), '==', Constant(231))), BinOp(Field('year'), '==', Constant(2000)))}
    
    inf = get_inference(input_table, df_oecd_normalized.subpipeline.paths[0], sub_op0_1.group_key, input_filters, output_filter)
    assert(inf.verify_correct(True))
test_2()