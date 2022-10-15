from ast import IsNot
import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

food = InitTable('discrim.csv')
food1 = Filter(food, AllAnd(*[IsNotNULL(Field('psoda')), IsNotNULL(Field('pfries')), IsNotNULL(Field('pentree'))]))
food2 = Filter(food1, AllAnd(*[IsNotNULL(Field('prppov')), IsNotNULL(Field('prpblck'))]))

sub_op0 = SubpipeInput(food2, 'group', ['chain'])
sub_op1 = AllAggregate(sub_op0, Value(0), 'lambda x: median(x["wagest"])')
sub_op2 = Filter()
sub_op3 = SetItemWithDependency(sub_op2, )
