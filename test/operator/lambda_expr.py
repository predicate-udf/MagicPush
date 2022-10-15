import os
import sys
import tokenize
import z3
import imp
import dis
sys.path.append('../../')
from predicate import *
from interface import *
from util import *
from util_type import *
from lambda_symbolic_exec.lambda_expr_eval import *

"""
op = SetItem(None, 'year', 'lambda row: row["file_date"].year')
op.input_schema = {'file_date':'datetime'}
inf = op.get_inference_instance(BinOp(Field(op.new_col), '==', Constant(1)))
inf.input_filters = [get_filter_replacing_field(inf.output_filter, {op.new_col: Expr(op.apply_func)})]
assert(inf.verify_correct())


op = SetItem(None, 'age', "lambda xxx__: str(xxx__['age_lower'])+'-'+str(xxx__['age_upper'])")
op.input_schema = {'age_lower':'str','age_upper':'str'}
inf = op.get_inference_instance(BinOp(Field(op.new_col), '==', Constant('11-15')))
inf.input_filters = [get_filter_replacing_field(inf.output_filter, {op.new_col: Expr(op.apply_func)})]
assert(inf.verify_correct())

op = SetItem(None, "RUL", "lambda xxx__: (xxx__['cycle_y']-xxx__['cycle_x'])" )
op.input_schema = {'cycle_y':'int','cycle_x':'int'}
inf = op.get_inference_instance(BinOp(Field(op.new_col), '==', Constant(12)))
inf.input_filters = [get_filter_replacing_field(inf.output_filter, {op.new_col: Expr(op.apply_func)})]
assert(inf.verify_correct())

op = SetItem(None, "person_hard_worker", 'lambda xxx__: 1 if xxx__[\'hours_worked_per_week\'] > 38.8 else 0')
op.input_schema = {'hours_worked_per_week':'float'}
inf = op.get_inference_instance(BinOp(Field(op.new_col), '==', Constant(0)))
#inf.input_filters = [get_filter_replacing_field(inf.output_filter, {op.new_col: Expr(op.apply_func)})]
inf.input_filters = [BinOp(Field('hours_worked_per_week'), '<=', Constant(38.8))]
assert(inf.verify_correct())

op = SetItem(None, "week", "lambda x: x['week'].str.extract(r'x(\d+)')")
op.input_schema = {'week':'str'}
inf = op.get_inference_instance(BinOp(Field(op.new_col), '==', Constant('33')))
inf.input_filters = [get_filter_replacing_field(inf.output_filter, {op.new_col: Expr(op.apply_func)})]
assert(inf.verify_correct())

op = SetItem(None, 'Employer_clean', "lambda x: re.sub('[^A-Za-z]+', '', str(x['Employer']).lower())")
op.input_schema = {'Employer':'str'}
inf = op.get_inference_instance(BinOp(Field(op.new_col), '==', Constant('someone')))
inf.input_filters = [get_filter_replacing_field(inf.output_filter, {op.new_col: Expr(op.apply_func)})]
assert(inf.verify_correct())



op = FillNA(None, "hours_worked_per_week", 0)
op.input_schema = {'hours_worked_per_week': 'int'}
inf = op.get_inference_instance(BinOp(Field('hours_worked_per_week'), '>', Constant(0)))
#inf.input_filters = [get_filter_replacing_field(inf.output_filter, {op.inference.new_col: Expr(op.inference.apply_func)})]
inf.input_filters = [get_filter_replacing_field(inf.output_filter, {op.inference.new_col: Expr(op.inference.apply_func)}, [op.col, op.fill_value])]
print(inf.input_filters)
assert(inf.verify_correct())


# op = SetItem(None, "published", 'lambda xxx__: xxx__["published"][0:10]')
# op.input_schema = {'published':'str'}
# inf = op.get_inference_instance(BinOp(Field("published"),'==',Constant("2016-10-26")))
# inf.input_filters = [BinOp(Field('published'),'==',Constant("2016-10-26"))]
# inf.verify_correct()

op = SetItem(None, 'Categories', "lambda xxx__: 'Finance' if 'Finance' in xxx__['Category Groups'] else \
('Biotechnology' if 'Biotechnology' in xxx__['Category Groups'] else \
('Health Care' if 'Health Care' in xxx__['Category Groups'] else 'Unknown'))", 'str')
op.input_schema = {'Category Groups':'str'}
inf = op.get_inference_instance(BinOp(Field("Categories"),'==', Constant('Finance')))
inf.input_filters = [BinOp(Constant('Finance'), 'subset', Field('Category Groups'))]
inf.verify_correct()


expr_str = "lambda x: True if not x['test']==1 else False"
expr_str = "lambda xxx__: 'Finance' if 'Finance' in xxx__['Category Groups'] else \
('Biotechnology' if 'Biotechnology' in xxx__['Category Groups'] else \
('Health Care' if 'Health Care' in xxx__['Category Groups'] else 'Unknown'))"
# expr_str = 'lambda x,y: 2 if y==1 else x'
# expr_str = "lambda x,y: x+str(int(y['product_id_latest_train'])) if y['product_id_latest_train'] > 0 else x"
# expr_str = 'lambda x,y: False if not y["wonMatchup"]==True else x'
exprs = get_predicate_from_lambda(expr_str)
print(','.join([str(e) for e in exprs]))
"""

import re
#op12_0 = Split(op11, "sex_and_age", ["0","1","2"], "(\D)(\d+)(\d{2})")
#op14 = SetItem(op13, 'age', "lambda xxx__: str(xxx__['age_lower'])+'-'+str(xxx__['age_upper'])")
lambda1 = "lambda xxx__: str(xxx__['age_lower'])+'-'+str(xxx__['age_upper'])"
lambda2 = 'lambda x: list(filter(None, re.split(r"(\D)(\d+)(\d{2})", x["sex_and_age"])))[1]'
lambda3 = 'lambda x: list(filter(None, re.split(r"(\D)(\d+)(\d{2})", x["sex_and_age"])))[2]'
s1 = replace_lambda_in_lambda('xxx__', 'age_lower', lambda1, lambda2)
print(s1)
s2 = replace_lambda_in_lambda('xxx__', 'age_upper', s1, lambda3)
print(s2)
ret = eval(s2)({'sex_and_age':'m4554'})
print(ret)
# replace xxx__['age_lower'] with lambda2 and xxx__['age_upper']
# [age] == '10-20'
# str(xxx__['age_lower'])+'-'+str(xxx__['age_upper']) == '10-20'
# str(re.split(r"(\D)(\d+)(\d{2})", x["sex_and_age"])[1]) + '-' + str(re.split(r"(\D)(\d+)(\d{2})", x["sex_and_age"])[2]) == '10-20'



