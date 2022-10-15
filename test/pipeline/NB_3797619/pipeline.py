
import sys
import numpy as np
from ppl_interface import *

op0 = InitTable("data_0.pickle")
op3 = InitTable("data_2.pickle")
op4 = DropDuplicate(op3, ["postcode"])
op5 = LeftOuterJoin(op0, op4, ["Postcode"],["postcode"])
op6 = DropColumns(op5, ["postcode"])
op8 = Rename(op6, { "Business Category":"business_category" })


