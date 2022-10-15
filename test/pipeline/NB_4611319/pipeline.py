
import sys
import numpy as np
from ppl_interface import *

op1 = InitTable("data_1.pickle")
op4 = SetItem(op1, "area", 'lambda xxx__: xxx__["Площадь, тыс.км2"]' )
op7 = SetItem(op4, "area", "lambda xxx__: xxx__['Площадь, тыс.км2'].replace(',','') if type(xxx__['Площадь, тыс.км2']) is str else '0'" )
op8 = ChangeType(op7, "float", "area", "area")
op12 = DropColumns(op7, [])
op13 = SetItem(op12, "density", 'lambda xxx__: (xxx__["Численность населения (2002  г.), чел"] / (1000*xxx__["area"]) if xxx__["area"]!=0 else 0 )' )

