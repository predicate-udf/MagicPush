from multiprocessing.connection import Pipe
import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *
from predicate import *
from generate_input_filters import *

import numpy as np
import random
import pickle

insurance = InitTable('insurance.csv')

sub_op0 = SubpipeInput(insurance, 'group', ['sex','smoker','region'])
sub_op_min = AllAggregate(sub_op0, Value(0), 'lambda x,y: x if x < y["charges"] else y["charges"]')
sub_op_max = AllAggregate(sub_op0, Value(0), 'lambda x,y: x if x > y["charges"] else y["charges"]')
sub_out = ScalarComputation({'min_':sub_op_min, 'max_':sub_op_max}, 'lambda min_, max_: str(min_)+","+str(max_)')
out = CogroupedMap(SubPipeline(PipelinePath([sub_op0, sub_op_min, sub_op_max, sub_out])))
