
import sys
import numpy as np
from ppl_interface import *

op0 = InitTable("data_0.pickle")
op1 = InitTable("data_1.pickle")
op2 = LeftOuterJoin(op0, op1, ["transaction_id"],["transaction_id"])
op3 = Rename(op2, { "app_name_x":"app_name","log_date_x":"log_date","test_name_x":"test_name","test_case_x":"test_case","user_id_x":"user_id","log_date_y":"click_date" })
op4 = DropColumns(op3, ["test_name","test_case_y","app_name","test_name_y","user_id_y","app_name_y"])
op6 = DropColumns(op4, [])
op71 = SetItem(op6, "是否被点击", "lambda xxx__: 1 if not pd.isnull(xxx__['click_date']) else 0")
op7 = Pivot(op6, 'test_case','是否被点击','user_id', "count", {'test_case':'str',0:'int',1:'int'})
op8 = SetItem(op7, "点击率", "lambda xxx__: (xxx__[1] / (xxx__[1]+xxx__[0]))" )
op9 = SetItem(op8, "点击率", "lambda xxx__: format(xxx__['点击率'], '.2%')" )
