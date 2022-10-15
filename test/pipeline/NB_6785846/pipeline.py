import sys
from interface import *


op0 = InitTable("contribution_processed.pickle")
op1 = InitTable("ma-companies-on-linkedin.pickle")

op2 = SetItem(op0, 'Employer_clean', "lambda x: re.sub('[^A-Za-z]+', '', str(x['Employer']).lower())")
op3 = SetItem(op1, 'Company_clean', "lambda x: re.sub('[^A-Za-z]+', '', str(x['Company name']).lower())")

sub_op0 = SubpipeInput(op3, 'table')
sub_op_row = SubpipeInput(op2, "row")
employer_dict = pickle.load('employer_dict.p')
sub_op1 = Filter(sub_op0, BinOp(Field('Company_clean'), '==', \
    ScalarComputation({'row':sub_op_row, 'employer_dict':employer_dict}, "lambda row, employer_dict: employer_dict[row['Employer_clean']]")))
sub_op2 = AllAggregate(sub_op1, Value(0), 'lambda x, row: row["Industry"]')
cond1 = ScalarComputation({'row':sub_op_row, 'employer_dict':employer_dict}, "lambda row, employer_dict: row['Employer_clean'] in employer_dict")
cond2 = ScalarComputation({'row':sub_op_row, 'employer_dict':employer_dict}, "lambda row, employer_dict: row['Employer_clean'] not in employer_dict")
sub_op3 = ScalarComputation({'row':sub_op_row}, 'lambda row: None')

op4 = CrosstableUDF(op2, 'Industry_new', SubPipeline([\
    PipelinePath([sub_op0, sub_op_row, sub_op1, sub_op2],cond1), \
    PipelinePath([sub_op_row, sub_op3], cond2)]))

op5 = DropColumns(op4, ['Employer_clean'])

