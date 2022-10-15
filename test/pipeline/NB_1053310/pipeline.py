
import sys
import numpy as np
from ppl_interface import *

op0 = InitTable("data_0.pickle")
op1 = InitTable("data_1.pickle")
op2 = Filter(op0, BinOp(Field("CULVERT_COND_062"),'==',Constant("N")))

op4 = Filter(op2, BinOp(Field("YEAR_BUILT_027"),'>',Constant(1900)))
op5 = Filter(op4, BinOp(Field("STRUCTURE_LEN_MT_049"),'<=',Constant(400)))
op6 = Filter(op1, BinOp(Field("elevation"),'>=',Constant(0)))
op7 = DropColumns(op6, ["Unnamed: 0"])
op8 = InnerJoin(op5, op7, ["LAT_016","LONG_017"],["LAT_016","LONG_017"])
op9 = DropNA(op8, ["STRUCTURE_KIND_043A"])
op10 = ChangeType(op9, 'float', "SUPERSTRUCTURE_COND_059", 'SUPERSTRUCTURE_COND_059')
op18 = GroupBy(op10, ["STRUCTURE_KIND_043A"], { "SUPERSTRUCTURE_COND_059":(Value(0, True),"mean") }, { "SUPERSTRUCTURE_COND_059":"SUPERSTRUCTURE_COND_059" })

