
import sys
import numpy as np


op10 = InitTable("data_21.pickle")
op11 = UnPivot(op10, ["country","year"], ["m5564","m2534","mu","m65","m4554","m1524","m3544","f014","m014"], "sex_and_age", "cases")
op12 = Split(op11, 'sex_and_age', ["0", "1", "2"], regex="(\D)(\d+)(\d{2})", by=None)
op13 = Rename(op12, { "0":"sex","1":"age_lower","2":"age_upper" })
#op15 = ConcatColumn([op11,op13])
op14 = DropColumns(op13, ['country','year', 'cases'])
op15 = InnerJoin(op11, op14, ['index'], ['index'])
op151 = SetItem(op15, 'age', 'lambda x: x["age_lower"] + "-" + x["age_upper"]')
op16 = DropColumns(op151, ["sex_and_age","age_lower","age_upper"])
op17 = DropNA(op16, ["country","year","cases","sex","age"])
op18 = SortValues(op17, ["country","year","sex","age"])
