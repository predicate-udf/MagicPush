
import sys
import numpy as np
from ppl_interface import *

op0 = InitTable("data_0.pickle")
op1 = DropColumns(op0, ["number"])
op2 = Rename(op1, {"country":"index"})
op3 = SetItem(op2, "lngd", "lambda xxx__: (xxx__['popgrowth']/100 + 0.05)")
op4 = SetItem(op3, "ls", "lambda xxx__: (xxx__['i_y']/100)")
op5 = SetItem(op4, "ls_lngd", 'lambda xxx__: (xxx__["ls"] - xxx__["lngd"])' )
