
import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *
from predicate import *
from generate_input_filters import *

import numpy as np
import random
import pickle



op0 = InitTable("bundas_train.pickle")
op1 = InitTable("bundas_test.pickle")
op2 = Append([op0, op1])
op3 = GroupBy(op2, ['Item_ID'], {'Weight': (Value(0, True), 'mean')}, {'Weight':'Weight'})
# row: left table; y: right table
#op4 = CrossTableUDF(op2, op3, 'Weight', \
#    lambda row: row['Weight'], lambda row, x, y: y['Weight'] if (pd.isnull(row['Weight']) and row['Item_ID']==y['Item_ID']) else x)
sub_op0 = SubpipeInput(op3, 'table')
sub_op_row = SubpipeInput(op2, 'row')
filter_row = ScalarComputation({'x':sub_op_row}, 'lambda x: x["Item_ID"]')
sub_op1 = Filter(sub_op0, BinOp(Field('Item_ID'), '==', filter_row))
sub_op2 = AllAggregate(sub_op1, Value(0), 'lambda x,y: y["Weight"]')
sub_op3 = ScalarComputation({'x':sub_op_row}, 'lambda x: x["Weight"]')
cond1 = ScalarComputation({'x':sub_op_row}, 'lambda x: pd.isnull(x["Weight"])')
cond2 = ScalarComputation({'x':sub_op_row}, 'lambda x: not pd.isnull(x["Weight"])')
op4 = CrosstableUDF(op2, 'Weight', \
    SubPipeline([PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, sub_op2], cond1), \
         PipelinePath([sub_op_row, sub_op3], cond2)]))

op5 = InitTable('store_size_mode.pickle')
#op6 = CrossTableUDF(op4, op5, 'Store_Size', \
#    lambda row: row['Store_Size'], lambda row, x, y: y['Store_Size'] if (pd.isnull(row['Store_Size']) and row['Store_Type']==y['Store_Type']) else y)
sub_op0 = SubpipeInput(op5, 'table')
sub_op_row = SubpipeInput(op4, 'row')
filter_row = ScalarComputation({'x':sub_op_row}, 'lambda x: x["Store_Type"]')
sub_op1 = Filter(sub_op0, BinOp(Field('Store_Type'), '==', filter_row))
sub_op2 = AllAggregate(sub_op1, Value(0), 'lambda x,y: y["Store_Size"]')
sub_op3 = ScalarComputation({'x':sub_op_row}, 'lambda x: x["Store_Size"]')
cond1 = ScalarComputation({'x':sub_op_row}, 'lambda x: pd.isnull(x["Store_Size"])')
cond2 = ScalarComputation({'x':sub_op_row}, 'lambda x: not pd.isnull(x["Store_Size"])')
op6 = CrosstableUDF(op4, 'Store_Size', \
    SubPipeline([PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, sub_op2], cond1), \
         PipelinePath([sub_op_row, sub_op3], cond2)]))


op7 = GroupBy(op2, ['Item_ID'], {'Visibility': (Value(0, True), 'mean')}, {'Visibility':'Visibility'})
#op8 = CrossTableUDF(op2, op3, 'Visibility', \
#    lambda row: row['Visibility'], lambda row, x, y: y['Visibility'] if (pd.isnull(row['Visibility']) and row['Item_ID']==y['Item_ID']) else x)
sub_op0 = SubpipeInput(op7, 'table')
sub_op_row = SubpipeInput(op6, 'row')
filter_row = ScalarComputation({'x':sub_op_row}, 'lambda x: x["Item_ID"]')
sub_op1 = Filter(sub_op0, BinOp(Field('Item_ID'), '==', filter_row))
sub_op2 = AllAggregate(sub_op1, Value(0), 'lambda x,y: y["Visibility"]')
sub_op3 = ScalarComputation({'x':sub_op_row}, 'lambda x: x["Visibility"]')
cond1 = ScalarComputation({'x':sub_op_row}, 'lambda x: pd.isnull(x["Visibility"])')
cond2 = ScalarComputation({'x':sub_op_row}, 'lambda x: not pd.isnull(x["Visibility"])')
op8 = CrosstableUDF(op6, 'Visibility', \
    SubPipeline([PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, sub_op2], cond1), \
         PipelinePath([sub_op_row, sub_op3], cond2)]))

op9 = SetItem(op8, 'FatContent', "lambda x: {'reg':'Regular','low fat':'Low Fat','LF':'Low Fat'}[x['FatContent']] if x['FatContent'] in ['reg','low fat','LF'] else x['FatContent']")
op10 = SetItem(op9, 'Item_Category', "lambda x: x['Item_ID'][0:2]")

ops = [op0, op1, op2, op3, op4, op5, op6, op7, op8, op9, op10]

output_schemas = generate_output_schemas(ops)
