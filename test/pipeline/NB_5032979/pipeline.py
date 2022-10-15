import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *
from predicate import *
from generate_input_filters import *

import numpy as np
import random
import pickle

df = InitTable('combined.csv')
df1 = SetItem(df, 'text', "lambda df: df['state'].astype(str) + '\\n' + 'Positive: ' + df['positive'].astype(str) + ', ' + 'Deaths: '+ df['death'].astype(str) + ', ' + 'Negative: '+ df['negative'].astype(str) + ', ' + 'Total Tested: '+ df['total'].astype(str)")
df2 = Filter(df1, BinOp(Field('state'), '!=', Constant('GU')))
df3 = Filter(df2, BinOp(Field('state'), '!=', Constant('PR')))
df4 = Filter(df3, BinOp(Field('state'), '!=', Constant('AS')))
df5 = Filter(df4, BinOp(Field('state'), '!=', Constant('MP')))
df6 = Filter(df5, BinOp(Field('state'), '!=', Constant('PRI')))
df7 = Filter(df6, BinOp(Field('state'), '!=', Constant('VIR')))
df8 = Filter(df7, BinOp(Field('state'), '!=', Constant('VI')))


sub_op0 = SubpipeInput(df8, 'group', ['state'])
sub_op1 = AllAggregate(sub_op0, Value(0),'lambda x,y: y["Emergency Declaration Date"]')
sub_op2 = Filter(sub_op0, BinOp(Field('date'),'>',sub_op1))
sub_op3 = AllAggregate(sub_op2, Value(0), 'lambda x,y: x+y["number_of_iclaims"]')
iclaims_since_emergency_by_state1 = CogroupedMap(SubPipeline(PipelinePath([sub_op0, sub_op1, sub_op2, sub_op3])))
iclaims_since_emergency_by_state = Rename(iclaims_since_emergency_by_state1, {"new_aggr_col":'sum'})

sub_op_row = SubpipeInput(df8, "row")
sub_op0 = SubpipeInput(iclaims_since_emergency_by_state, 'table')
row_state = ScalarComputation({'row':sub_op_row}, 'lambda row: row["state"]')
sub_op1 = Filter(sub_op0, BinOp(Field("state"), '==', row_state))
cond_aggr = AllAggregate(sub_op1, Value(0), "lambda x:1")
cond1 = ScalarComputation({'count':cond_aggr}, 'lambda count: count>0')
cond2 = ScalarComputation({'count':cond_aggr}, 'lambda count: count==0')
retv1 = AllAggregate(sub_op1, Value(0), "lambda x,y: y['sum']")
retv2 = ScalarComputation({'row':sub_op_row}, 'lambda row: None')
df9 = CrosstableUDF(df8, 'iclaims_since_emergency', SubPipeline([\
    PipelinePath([sub_op_row, sub_op0, row_state, sub_op1, cond_aggr, retv1],cond1), \
    PipelinePath([sub_op_row, sub_op0, row_state, sub_op1, cond_aggr, cond2, retv2], cond2)]))

sub_op0 = SubpipeInput(df9, 'group', ['state'])
sub_op1 = AllAggregate(sub_op0, Value(0),'lambda x,y: y["Stay At Home Order Date"]')
sub_op2 = Filter(sub_op0, BinOp(Field('date'),'>',sub_op1))
sub_op3 = AllAggregate(sub_op2, Value(0), 'lambda x,y: x+y["number_of_iclaims"]')
iclaims_since_stayathome_by_state1 = CogroupedMap(SubPipeline(PipelinePath([sub_op0, sub_op1, sub_op2, sub_op3])))
iclaims_since_stayathome_by_state = Rename(iclaims_since_stayathome_by_state1, {"new_aggr_col":'sum'})

sub_op_row = SubpipeInput(df9, "row")
sub_op0 = SubpipeInput(iclaims_since_stayathome_by_state, 'table')
row_state = ScalarComputation({'row':sub_op_row}, 'lambda row: row["state"]')
sub_op1 = Filter(sub_op0, BinOp(Field("state"), '==', row_state))
cond_aggr = AllAggregate(sub_op1, Value(0), "lambda x:1")
cond1 = ScalarComputation({'count':cond_aggr}, 'lambda count: count>0')
cond2 = ScalarComputation({'count':cond_aggr}, 'lambda count: count==0')
retv1 = AllAggregate(sub_op1, Value(0), "lambda x,y: y['sum']")
retv2 = ScalarComputation({'row':sub_op_row}, 'lambda row: None')
df9_1 = CrosstableUDF(df8, 'iclaims_since_stayathome', SubPipeline([\
    PipelinePath([sub_op_row, sub_op0, row_state, sub_op1, cond_aggr, retv1],cond1), \
    PipelinePath([sub_op_row, sub_op0, row_state, sub_op1, cond_aggr, cond2, retv2], cond2)]))

df10 = SortValues(df9_1, ['date'])
df11 = Filter(df10, BinOp(Field('date'), ">=", Constant("2020-01-30")))
df12 = Filter(df11, IsNotNULL(Field('death')))
df13 = Filter(df12, IsNotNULL(Field('positive')))
df14 = SetItem(df13, "pct_death_over_positive", "lambda df: 100*(df['death']/df['positive'])")
df15 = SetItem(df14, "death_rate", "lambda df: df['death']/df['positive']")
df16 = SetItem(df15, "high_pct_death", "lambda df: lambda x: '1%+ death rate' if df['pct_death_over_positive'] >= 1 else 'below 1% death rate'")
df17 = SetItem(df16, "pct_positive_out_of_total_tested", "lambda df: 100*(df['positive']/df['total'])")
df18 = SetItem(df17, "positive_over_pop", "lambda df: (df['positive']/df['POPESTIMATE2019'])*100")
df19 = SetItem(df18, "positive_per_100k", "lambda df: (df['positive_over_pop']/100)*100000")
df20 = SetItem(df19, "pct_pop_tested", "lambda df: (df['total']/df['POPESTIMATE2019'])*100")
