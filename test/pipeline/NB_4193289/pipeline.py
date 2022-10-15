
import sys
import numpy as np
from ppl_interface import *

op0 = InitTable("data_0.pickle")
op1 = InitTable("data_1.pickle")
op3 = DropColumns(op0, ["Id"])
op4 = DropColumns(op1, ["Id"])
op7 = DropColumns(op3, ["Alley","FireplaceQu","PoolQC","Fence","MiscFeature"])
op8 = DropColumns(op4, ["Alley","FireplaceQu","PoolQC","Fence","MiscFeature"])
op14 = Append([op7,op8])
