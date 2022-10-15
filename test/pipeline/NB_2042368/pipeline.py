import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *
from predicate import *
from generate_input_filters import *

import numpy as np
import random
import pickle
orders = InitTable('orders.csv')
train = InitTable('order_products__train.csv')

otrain = Filter(orders, BinOp(Field('eval_set'), '==', Constant('train')))
Alltrain = InnerJoin(otrain, train, ['order_id'], ['order_id'])
ytrain1  = DropColumns(Alltrain, ['order_id', 'eval_set', 'order_number', 'order_dow',\
       'order_hour_of_day', 'days_since_prior_order', 'add_to_cart_order', 'reordered'])
ytrain = Rename(ytrain1, {'product_id':'product_id_latest_train'})

sub_op0 = SubpipeInput(ytrain, 'group', ['user_id'])
sub_op1 = AllAggregate(sub_op0, Value(''), "lambda x,y: x+str(int(y['product_id_latest_train'])) if y['product_id_latest_train'] > 0 else x")
cond1 = ScalarComputation({'out':sub_op1}, "lambda out: out != ''")
cond2 = ScalarComputation({'out':sub_op1}, "lambda out: out == ''")
out = ScalarComputation({'out':sub_op1}, "lambda out: out.rstrip()")
none_out = ScalarComputation({'x':sub_op1}, "lambda x: None")

train_order = CogroupedMap(SubPipeline([\
    PipelinePath([sub_op0, sub_op1, out], cond1), \
    PipelinePath([sub_op0, sub_op1, none_out], cond2)]))
