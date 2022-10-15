import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *
from predicate import *
from generate_input_filters import *

import numpy as np
import random
import pickle

real_wage = InitTable('real-wage.csv')
real_wage1 = Filter(real_wage, BinOp(Field('Series'), '==', Constant('In 2018 constant prices at 2018 USD PPPs')))
real_wage2 = DropColumns(real_wage1, ['COUNTRY', 'SERIES', 'Series', 'TIME', 'Unit Code',\
       'Unit', 'PowerCode Code', 'PowerCode', 'Reference Period Code',\
       'Reference Period', 'Flag Codes', 'Flags'])

sub_op0 = SubpipeInput(real_wage2, 'group', ['Country'])
sub_op1 = Filter(sub_op0, BinOp(Field('Time'), '==', Constant(2007)))
x = AllAggregate(sub_op1, Value(0), "lambda x,y: y['Value']")
sub_op2 = SetItemWithDependency(sub_op0, 'Value', {'df':sub_op0, 'x':x}, 'lambda df,x: df["Value"]/x')
wage_index = CogroupedMap(SubPipeline(PipelinePath([sub_op0, sub_op1, x, sub_op2])))

data = Filter(wage_index, And(BinOp(Field('Country'), 'in', Constant(['Greece','United Kingdom','United States','France','Germany','Poland'])), \
       BinOp(Field('Time'), '>=', Constant(2007))))
