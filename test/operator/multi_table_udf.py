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
import pandas as pd

"""
LeftTableLoopRightOp
for row in left_table:
   emit( Op(row, right_table) )
"""
def prepare(left_columns=[], right_columns=[]):
    schema_l = {k:'int' for k in left_columns} #generate_random_schema(N_cols)
    schema_r = {k:'int' for k in right_columns}
    input_table_l = generate_symbolic_table('t1',schema_l, 1)
    input_table_r = generate_symbolic_table('t2',schema_r, 2)
    
    return schema_l, schema_r, input_table_l, input_table_r

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


def get_inference(input_table_left, path, new_col, input_filters, output_filter, output_exist=False):
    
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
            if op.input_type == 'row':
                op.inference.input_tables = [input_table_left]
        # TODO for yin: get input filter for this subpipeline operator
        inference.input_filters = input_filters[op] if type(input_filters[op]) is list else [input_filters[op]] 
        
        sub_pipeline[i] = inference
        sub_pipeline_dependency[i] = [op_id_map[op_] if op_ in op_id_map else [] for op_ in op.dependent_ops]
    
    path_inf = CrosstableUDFOnePathInference(input_table_left, new_col, sub_pipeline, sub_pipeline_dependency, \
            True if type(path.cond) is bool else sub_pipeline[-1], output_filter)
    if output_exist:
        path_inf.output_exists()

    return path_inf

"""
def imput_age(cols):
    Age = cols[0]
    Pclass = cols[1]
    if pd.isnull(Age):
        return titanic_data[titanic_data["Pclass"] == Pclass]["Age"].mean() 
        #return titanic_data[titanic_data["Pclass"] == Pclass]["Age"].sum()
    else:
        return Age
"""
def test_1():
    schema_l, schema_r, input_table_l, input_table_r = prepare(['Age','Pclass','col1'],['Age','Pclass','col2'])
    sub_op0 = SubpipeInput(None, 'table')
    sub_op0.input_schema = schema_r
    sub_op_row = SubpipeInput(None, 'row')
    # YIn: typo?
    sub_op_row.input_schema = schema_l
    filter_row = ScalarComputation({'x':sub_op_row}, 'lambda x: x["Pclass"]')
    sub_op1 = Filter(sub_op0, BinOp(Field('Pclass'), '==', filter_row))
    sub_op_sum = AllAggregate(sub_op1, Value(0), 'lambda x,y: x+y["Age"]')
    sub_op_count = AllAggregate(sub_op1, Value(0), 'lambda x,y: x+1')
    sub_op2 = ScalarComputation({'s':sub_op_sum,'c':sub_op_count}, 'lambda s,c: s/c')
    sub_op3 = ScalarComputation({'x':sub_op_row}, 'lambda x: x["Age"]')
    cond1 = ScalarComputation({'x':sub_op_row}, 'lambda x: pd.isnull(x["Age"])')
    cond2 = ScalarComputation({'x':sub_op_row}, 'lambda x: !pd.isnull(x["Age"])')
    # op2 = CrosstableUDF(None, 'Age', \
    #     SubPipeline([PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, sub_op_sum, sub_op_count, sub_op2], cond1), \
    #         PipelinePath([sub_op_row, sub_op3], cond2)]))
    op2 = CrosstableUDF(None, 'Age', \
        SubPipeline(PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, sub_op_sum, sub_op_count, sub_op2])))
   
    input_filters = {sub_op2:[True, True], sub_op3:True, sub_op_count:True, sub_op_sum:True, sub_op1:BinOp(Field("Pclass"),'==',Constant(1)), \
        filter_row: True, cond1: True, cond2: True, \
        sub_op_row:AllAnd(*[BinOp(Field("Pclass"),'==',Constant(1)), BinOp(Field("col1"),'==',Constant(10))]), sub_op0:BinOp(Field("Pclass"),'==',Constant(1))}

    """
    # this case is correct
    output_filter = AllAnd(*[BinOp(Field("Pclass"),'==',Constant(1)), BinOp(Field("col1"),'==',Constant(10))])
    inf = get_inference(input_table_l, op2.subpipeline.paths[0], op2.new_col, input_filters, output_filter, output_exist=False) # no guarantee output exist after output_filter
    assert(inf.verify_correct())
    """

    # replace output filter with the following, verifier would tell it is wrong, as input has no guarantee that the mean Age is 30
    output_filter = AllAnd(*[BinOp(Field('Age'),'==',Constant(30)), BinOp(Field("Pclass"),'==',Constant(1)), BinOp(Field("col1"),'==',Constant(10))])
    inf = get_inference(input_table_l, op2.subpipeline.paths[0], op2.new_col, input_filters, output_filter)
    assert(inf.verify_correct()==False)
    
    """
    # when adding a constraint that output exist, this case becomes correct
    output_filter = AllAnd(*[BinOp(Field('Age'),'==',Constant(30)), BinOp(Field("Pclass"),'==',Constant(1)), BinOp(Field("col1"),'==',Constant(10))])
    inf = get_inference(input_table_l, op2.subpipeline.paths[0], op2.new_col, input_filters, output_filter, output_exist=True)
    assert(inf.verify_correct())
    """

# Yin: filter rewrite? remove age from the filter.
#test_1()
# exit(0)

# left: Or((Age ==30 and Pclass ==1 and col1 ==10),(Age ==null and Pclass ==1 and col1 ==10))
# right: Pclass ==1



"""
hospitalProfiles['Estimated Ventilators'] = hospitalProfiles.apply(calcVentilators, axis=1)
def calcVentilators(row):
    beds = float(row['Beds'])
    state = row['State']
    ventilators = 0.0
    try:
        vpb = float(perCapitaResources[perCapitaResources['State']==state]['Ventilators/Bed'])
        ventilators = beds * vpb
    except:
        return ventilators        
    return ventilators
"""

def test_2():
    schema_l, schema_r, input_table_l, input_table_r = prepare(['Beds','State','col1'],['State','Ventilators/Bed','col2'])

    sub_op0 = SubpipeInput(None, 'table')
    sub_op0.input_schema = schema_r
    sub_op_row = SubpipeInput(None, 'row')
    sub_op_row.input_schema = schema_l
    filter_row = ScalarComputation({'x':sub_op_row}, 'lambda x: x["State"]')
    sub_op1 = Filter(sub_op0, BinOp(Field('State'), '==', filter_row))
    cond_helper = AllAggregate(sub_op1, Value(0), 'lambda x,y: x+1')
    cond1 = ScalarComputation({'tup_count':cond_helper}, 'lambda tup_count: tup_count==1')
    cond2 = ScalarComputation({'tup_count':cond_helper}, 'lambda tup_count: tup_count!=1')
    other_value = ScalarComputation({'x':sub_op_row},'lambda x: 0.0')
    sub_op2 = AllAggregate(sub_op1, Value(0, True), 'lambda x,y: y["Ventilators/Bed"]')
    sub_op3 = ScalarComputation({'beds': sub_op_row ,'vpb': sub_op2}, 'lambda beds,vpb: beds["Beds"]*vpb')
    op10 = CrosstableUDF(None, 'Estimated Ventilators',\
        SubPipeline([PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, sub_op2, sub_op3], cond1), \
            PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, other_value], cond2)]))
    

    """
    # the following is correct
    input_filters = {sub_op3:[True,True],sub_op2:True,other_value:True,cond_helper:True,filter_row:True,other_value:True,cond1:True,cond2:True,\
        sub_op1:AllAnd(*[BinOp(Field('State'),'==',Constant(10))]),
        sub_op_row:AllAnd(*[BinOp(Field('Beds'),'==',Constant(2)), BinOp(Field('State'),'==',Constant(10)), BinOp(Field('col1'),'==',Constant(1000))])}
    input_filters[sub_op0] = input_filters[sub_op1]
    output_filter = AllAnd(*[BinOp(Field('Beds'),'==',Constant(2)), BinOp(Field('State'),'==',Constant(10)), BinOp(Field('col1'),'==',Constant(1000))])
    inf = get_inference(input_table_l, op10.subpipeline.paths[0], op10.new_col, input_filters, output_filter, output_exist=False)
    assert(inf.verify_correct())


    # the following combination is incorrect as the input filter on the right table does not guarantee 'Estimated Ventilators'==8
    input_filters = {sub_op3:[True,True],sub_op2:True,other_value:True,cond_helper:True,filter_row:True,other_value:True,cond1:True,cond2:True,\
        sub_op1:AllAnd(*[BinOp(Field('State'),'==',Constant(10)), BinOp(Field('Ventilators/Bed'), '==', Constant(4))]),
        sub_op_row:AllAnd(*[BinOp(Field('Beds'),'==',Constant(2)), BinOp(Field('State'),'==',Constant(10)), BinOp(Field('col1'),'==',Constant(1000))])}
    input_filters[sub_op0] = input_filters[sub_op1]
    output_filter = AllAnd(*[BinOp(Field('Beds'),'==',Constant(2)), BinOp(Field('State'),'==',Constant(10)), BinOp(Field('col1'),'==',Constant(1000)),\
                       BinOp(Field('Estimated Ventilators'),'==',Constant(8))])
    inf = get_inference(input_table_l, op10.subpipeline.paths[0], op10.new_col, input_filters, output_filter, output_exist=False)
    assert(inf.verify_correct()==False)

    # it becomes correct when the output after output_filter is non-empty!
    input_filters = {sub_op3:[True,True],sub_op2:True,other_value:True,cond_helper:True,filter_row:True,other_value:True,cond1:True,cond2:True,\
        sub_op1:AllAnd(*[BinOp(Field('State'),'==',Constant(10)), BinOp(Field('Ventilators/Bed'), '==', Constant(4))]),
        sub_op_row:AllAnd(*[BinOp(Field('Beds'),'==',Constant(2)), BinOp(Field('State'),'==',Constant(10)), BinOp(Field('col1'),'==',Constant(1000))])}
    input_filters[sub_op0] = input_filters[sub_op1]
    output_filter = AllAnd(*[BinOp(Field('Beds'),'==',Constant(2)), BinOp(Field('State'),'==',Constant(10)), BinOp(Field('col1'),'==',Constant(1000)),\
                       BinOp(Field('Estimated Ventilators'),'==',Constant(8))])
    inf = get_inference(input_table_l, op10.subpipeline.paths[0], op10.new_col, input_filters, output_filter, output_exist=True)
    assert(inf.verify_correct())


    # the following combination is also correct when the output after output_filter is non-empty
    input_filters = {sub_op3:[True,True],sub_op2:True,other_value:True,cond_helper:True,filter_row:True,other_value:True,cond1:True,cond2:True,\
        sub_op1:AllAnd(*[BinOp(Field('State'),'==',Constant(10))]),
        sub_op_row:AllAnd(*[BinOp(Field('Beds'),'==',Constant(2)), BinOp(Field('State'),'==',Constant(10)), BinOp(Field('col1'),'==',Constant(1000))])}
    input_filters[sub_op0] = input_filters[sub_op1]
    output_filter = AllAnd(*[BinOp(Field('Beds'),'==',Constant(2)), BinOp(Field('State'),'==',Constant(10)), BinOp(Field('col1'),'==',Constant(1000)),\
                       BinOp(Field('Estimated Ventilators'),'==',Constant(8))])
    inf = get_inference(input_table_l, op10.subpipeline.paths[0], op10.new_col, input_filters, output_filter, output_exist=True)
    assert(inf.verify_correct())
    """

    # if beds["Beds"]*vpb > 0, and the output filter has "Estimated Ventilators"==0
    input_filters = {sub_op3:[False,False],sub_op2:False,other_value:True,cond_helper:True,\
        filter_row:True,cond1:True,cond2:True,\
        sub_op1:False,
        sub_op_row:AllAnd(*[BinOp(Field('Beds'),'==',Constant(2)), BinOp(Field('State'),'==',Constant(10)), BinOp(Field('col1'),'==',Constant(1000))])}
    input_filters[sub_op0] = input_filters[sub_op1]
    output_filter = AllAnd(*[BinOp(Field('Beds'),'==',Constant(2)), BinOp(Field('State'),'==',Constant(10)), BinOp(Field('col1'),'==',Constant(1000)), \
                    BinOp(Field('Estimated Ventilators'),'==',Constant(0))])
    inf = get_inference(input_table_l, op10.subpipeline.paths[0], op10.new_col, input_filters, output_filter, output_exist=True)
    # add constraint, making beds["Beds"]*vpb > 0 
    inf.input_constraint = z3.And(inf.input_constraint, \
        input_table_l[0]['Beds'].v>0, z3.And(*[row['Ventilators/Bed'].v > 0 for row in sub_op0.inference.input_tables[0]])) 
    assert(inf.verify_correct())
    

    # checking the other path
    inf = get_inference(input_table_l, op10.subpipeline.paths[1], op10.new_col, input_filters, output_filter, output_exist=True)
    inf.input_constraint = z3.And(inf.input_constraint, \
        input_table_l[0]['Beds'].v>0, z3.And(*[row['Ventilators/Bed'].v > 0 for row in sub_op0.inference.input_tables[0]])) 
    assert(inf.verify_correct())
# test_2()
# exit(0)

def test_3():

    schema_l, schema_r, input_table_l, input_table_r = prepare(['City','col1'],['City','Lat','col2'])

    sub_op0 = SubpipeInput(None, 'table')
    sub_op0.input_schema = schema_r
    sub_op_row = SubpipeInput(None, 'row')
    sub_op_row.input_schema = schema_l
    filter_row = ScalarComputation({'row':sub_op_row},"lambda row: row['City']")
    sub_op1 = Filter(sub_op0, BinOp(Field('City'), '==', filter_row))
    sub_op2 = AllAggregate(sub_op1, Value(0), "lambda x,y: y['Lat']")
    cond_helper = AllAggregate(sub_op1, Value(0), "lambda x,y: x+1")
    cond1 = ScalarComputation({'count':cond_helper}, 'lambda count: count>0')
    cond2 = ScalarComputation({'count':cond_helper}, 'lambda count: count==0')
    rs2 = ScalarComputation({'row':sub_op_row}, 'lambda row: 0')
    newDF1 = CrosstableUDF(None, "Lat", SubPipeline([\
        PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, sub_op2], cond1), \
        PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, rs2], cond2)]))

    f = BinOp(Field('City'), '==', Constant(1))
    input_filters = {sub_op0:f, sub_op_row:f, sub_op1:f, filter_row:f, sub_op2:True, cond_helper:True,cond1:True,cond2:True,rs2:True,\
        newDF1:f}
    output_filter = f
    inf = get_inference(input_table_l, newDF1.subpipeline.paths[0], newDF1.new_col, input_filters, output_filter)
    assert(inf.verify_correct())

    inf = get_inference(input_table_l, newDF1.subpipeline.paths[1], newDF1.new_col, input_filters, output_filter)
    assert(inf.verify_correct())

#test_3()

def test_4():
    schema_l, schema_r, input_table_l, input_table_r = prepare(['Country Code','col1'], ['CountryCode','GroupCode','col2'])
    sub_op0 = SubpipeInput(None, 'table')
    sub_op0.input_schema = schema_r
    sub_op_row = SubpipeInput(None, 'row')
    sub_op_row.input_schema = schema_l
    filter_row = ScalarComputation({'row':sub_op_row}, "lambda row: row['Country Code']")
    oecd_countries = Filter(sub_op0, \
        And(BinOp(Field('GroupCode'), '==', Constant(1)), \
            BinOp(Field('CountryCode'), '==', filter_row)))
    exists = AllAggregate(oecd_countries, Value(0), "lambda x,y: 1")
    op1 = CrosstableUDF(None, 'isin_country', \
        SubPipeline(PipelinePath([sub_op0, sub_op_row, filter_row, oecd_countries, exists])))

    input_filters = {sub_op0:And(BinOp(Field('GroupCode'),'==',Constant(1)), BinOp(Field('CountryCode'),'==',Constant(5))),\
        sub_op_row:BinOp(Field('Country Code'),'==',Constant(5)),\
            filter_row:None, oecd_countries:None,exists:None}
    output_filter = And(BinOp(Field('isin_country'), '==', Constant(1)), BinOp(Field('Country Code'),'==',Constant(5)))
    inf = get_inference(input_table_l, op1.subpipeline.paths[0], op1.new_col, input_filters, output_filter)
    inf.input_filters = [input_filters[sub_op_row], input_filters[sub_op0]]
    assert(inf.verify_correct(True))
test_4()

"""
def predict_price(new_listing):
    temp_df = train_df.copy()
    temp_df['distance'] = temp_df['accommodates'].apply(lambda x: np.abs(x - new_listing))
    temp_df = temp_df.sort_values('distance')
    nearest_neighbor_prices = temp_df.iloc[0:5]['price']
    
    predicted_price = nearest_neighbor_prices.mean()
    return(predicted_price)

for tup1 in test_df:
    train_df.apply().sort().top[:5].mean()
"""
initv = (0.0, 0)
lambda row, x, y: (x[0] + y['price'], x[0]+1) if x[1] < 5 else x

"""
df_contribuion['Industry_new'] = df_contribuion['Employer_clean'].apply(com_search)
def com_search(name):
    if name in employer_dict.keys():
        industry = df_companies[df_companies['Company_clean'] == employer_dict[name]]['Industry'].values[0]
        return industry
"""
initv = 0.0
lambda row, x, y: y['Industry'] if  (row['name'] == Constant(3) and y['Company_clean'] == row['name']) else x

"""
data['Weight'] = data[['Weight','Item_ID']].apply(impute_weight,axis=1).astype(float)
def impute_weight(cols):
    Weight = cols[0]
    Identifier = cols[1]
    
    if pd.isnull(Weight):
        return item_avg_weight['Weight'][item_avg_weight.index == Identifier]
    else:
        return Weight
"""
# #initv = row['Weight']
# lambda row, x, y: y['Weight'] if y['index']==row['Item_ID'] else x
# def test_3():
#     schema_l, schema_r, input_table_l, input_table_r = prepare(['Age','Pclass','col1'],['Age','Pclass','col2'])
#     sub_op0 = SubpipeInput(None, 'table')
#     sub_op0.input_schema = schema_r
#     sub_op_row = SubpipeInput(None, 'row')
#     # YIn: typo?
#     sub_op_row.input_schema = schema_l
#     filter_row = ScalarComputation({'x':sub_op_row}, 'lambda x: x["Pclass"]')
#     sub_op1 = Filter(sub_op0, BinOp(Field('Pclass'), '==', filter_row))
#     sub_op_sum = AllAggregate(sub_op1, Value(0), 'lambda x,y: x+y["Age"]')
#     sub_op_count = AllAggregate(sub_op1, Value(0), 'lambda x,y: x+1')
#     sub_op2 = ScalarComputation({'s':sub_op_sum,'c':sub_op_count}, 'lambda s,c: s/c')
#     sub_op3 = ScalarComputation({'x':sub_op_row}, 'lambda x: x["Age"]')
#     cond1 = ScalarComputation({'x':sub_op_row}, 'lambda x: pd.isnull(x["Age"])')
#     cond2 = ScalarComputation({'x':sub_op_row}, 'lambda x: !pd.isnull(x["Age"])')
#     op2 = CrosstableUDF(None, 'Age', \
#         SubPipeline([PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, sub_op_sum, sub_op_count, sub_op2], cond1), \
#             PipelinePath([sub_op_row, sub_op3], cond2)]))
    
   
#     input_filters = {sub_op2:[True, True], sub_op3:True, sub_op_count:True, sub_op_sum:True, sub_op1:BinOp(Field("Pclass"),'==',Constant(1)), \
#         filter_row: True,\
#         sub_op_row:And(And(BinOp(Field("Pclass"),'==',Constant(1)), BinOp(Field("col1"),'==',Constant(10)), Or(BinOp(Field("Age"),'==',Constant(30), IsNULL())) ), sub_op0:BinOp(Field("Pclass"),'==',Constant(1))}

    # this case is correct
    # output_filter = AllAnd(*[BinOp(Field("Pclass"),'==',Constant(1)), BinOp(Field("col1"),'==',Constant(10))])
    # inf = get_inference(input_table_l, op2.subpipeline.paths[0], op2.new_col, input_filters, output_filter, output_exist=False) # no guarantee output exist after output_filter
    # assert(inf.verify_correct())

    # # replace output filter with the following, verifier would tell it is wrong, as input has no guarantee that the mean Age is 30
    # output_filter = AllAnd(*[BinOp(Field('Age'),'==',Constant(30)), BinOp(Field("Pclass"),'==',Constant(1)), BinOp(Field("col1"),'==',Constant(10))])
    # inf = get_inference(input_table_l, op2.subpipeline.paths[0], op2.new_col, input_filters, output_filter)
    # assert(inf.verify_correct()==False)
    
    # # when adding a constraint that output exist, this case becomes correct
    # output_filter = AllAnd(*[BinOp(Field('Age'),'==',Constant(30)), BinOp(Field("Pclass"),'==',Constant(1)), BinOp(Field("col1"),'==',Constant(10))])
    # inf = get_inference(input_table_l, op2.subpipeline.paths[0], op2.new_col, input_filters, output_filter, output_exist=True)
    # assert(inf.verify_correct())