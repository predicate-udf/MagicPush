
import sys
import numpy as np
from ppl_interface import *

op2 = InitTable("data_1.pickle")
op3 = InitTable("data_2.pickle")
op4 = InitTable("data_3.pickle")
op5 = Append([op3,op4])
op6 = InitTable("data_5.pickle")
op7 = Append([op5,op6])
op8 = InitTable("data_7.pickle")
op9 = Append([op7,op8])
op10 = Append([op2,op9])
