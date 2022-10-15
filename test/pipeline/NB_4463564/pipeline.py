import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *


op0 = InitTable("ventilators.pickle")
op1 = SetItem(op0, 'State', "lambda x: {'Alabama':'AL','Alaska':'AK','Arizona':'AZ','Arkansas':'AR','California':'CA','Colorado':'CO','Connecticut':'CT','Delaware':'DE','District of Columbia':'DC','Florida':'FL','Georgia':'GA','Hawaii':'HI','Idaho':'ID','Illinois':'IL','Indiana':'IN','Iowa':'IA','Kansas':'KS','Kentucky':'KY','Louisiana':'LA','Maine':'ME','Maryland':'MD','Massachusetts':'MA','Michigan':'MI','Minnesota':'MN','Mississippi':'MS','Missouri':'MO','Montana':'MT','Nebraska':'NE','Nevada':'NV','New Hampshire':'NH','New Jersey':'NJ','New Mexico':'NM','New York':'NY','North Carolina':'NC','North Dakota':'ND','Northern Mariana Islands':'MP','Ohio':'OH','Oklahoma':'OK','Oregon':'OR','Palau':'PW','Pennsylvania':'PA','Puerto Rico':'PR','Rhode Island':'RI','South Carolina':'SC','South Dakota':'SD','Tennessee':'TN','Texas':'TX','Utah':'UT','Vermont':'VT','Virgin Islands':'VI','Virginia':'VA','Washington':'WA','West Virginia':'WV','Wisconsin':'WI','Wyoming':'WY','Guam':'GM'}[x['State']]")
op2 = InitTable("hospitals_beds.pickle")
op3 = GroupBy(op2, ['State'], {'Beds':(Value(0, True), 'sum')}, {'Beds':'Beds'})
op4 = InnerJoin(op1, op3, ['State'],['State'])
op5 = SetItem(op4, 'Ventilators/Bed', "lambda x: x['Ventilators']/x['Beds']")
op6 = InitTable("stateStats.pickle")
op7 = InnerJoin(op5, op6, ['State'], ['State'])
op8 = SetItem(op7, 'Beds/100,000', "lambda x: x['Beds']/x['Population']*100000")
op9 = SetItem(op8, 'Ventilators/100,000', "lambda x: x['Ventilators']/x['Population']*100000")
#op10 = CrossTableUDF(op5, op9, 'Estimated Ventilators', \
#    lambda row: 0.0 , lambda row, x, y: (y['Ventilators/Bed'] * row['Beds']) if row['State']==y['State'] else x)
sub_op0 = SubpipeInput(op9, 'table')
sub_op_row = SubpipeInput(op5, 'row')
filter_row = ScalarComputation({'x':sub_op_row}, 'lambda x: x["State"]')
sub_op1 = Filter(sub_op0, BinOp(Field('State'), '==', filter_row))
cond_helper = AllAggregate(sub_op1, Value(0), 'lambda x,y: x+1')
cond1 = ScalarComputation({'tup_count':cond_helper}, 'lambda tup_count: tup_count==1')
cond2 = ScalarComputation({'tup_count':cond_helper}, 'lambda tup_count: tup_count!=1')
other_value = ScalarComputation({'x':sub_op_row},'lambda x: 0.0')
sub_op2 = AllAggregate(sub_op1, Value(0, True), 'lambda x,y: y["Ventilators/Bed"]')
sub_op3 = ScalarComputation({'beds': sub_op_row ,'vpb': sub_op2}, 'lambda beds,vpb: beds["Beds"]*vpb')
op10 = CrosstableUDF(op5, 'Estimated Ventilators',\
    SubPipeline([PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, sub_op2, sub_op3], cond1), \
        PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, other_value], cond2)]))

#op11 = CrossTableUDF(op10, op9, 'Statewide Ventilators per Bed', \
#    lambda row: 0.0, lambda row, x, y: (y['Ventilators/Bed']) if row['State']==y['State'] else x)
sub_op0 = SubpipeInput(op9, 'table')
sub_op_row = SubpipeInput(op10, 'row')
filter_row = ScalarComputation({'x':sub_op_row}, 'lambda x: x["State"]')
sub_op1 = Filter(sub_op0, BinOp(Field('State'), '==', filter_row))
cond_helper = AllAggregate(sub_op1, Value(0), 'lambda x,y: x+1')
cond1 = ScalarComputation({'tup_count':cond_helper}, 'lambda tup_count: tup_count==1')
cond2 = ScalarComputation({'tup_count':cond_helper}, 'lambda tup_count: tup_count!=1')
other_value = ScalarComputation({'x':sub_op_row},'lambda x: 0.0')
sub_op2 = AllAggregate(sub_op1, Value(0, True), 'lambda x,y: y["Ventilators/Bed"]')
op10 = CrosstableUDF(op5, 'Statewide Ventilators per Bed',\
    SubPipeline([PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, sub_op2], cond1), \
        PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, other_value], cond2)]))
