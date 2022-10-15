import sys
sys.path.append("../../../")
sys.path.append("../../")
import z3
import dis
from interface import *
from util import *
import random
from predicate import *
from generate_input_filters import *
from compare_pushdown_result import get_output_filter, check_pushdown_result
import os

op2 = InitTable("data_1.pickle")
op3 = DropColumns(op2, ["Unnamed: 0","Organization Name URL"])
op5 = DropDuplicate(op3, ["Organization Name"])
op6 = Filter(op5, Not(IsNULL(Field("Group Gender"))))
op12 = Filter(op6, Not(IsNULL(Field("Category Groups"))))
op13 = Filter(op12, Not(IsNULL(Field("Headquarters Regions"))))
op14 = SetItem(op13, 'Categories', "lambda xxx__: 'Finance' if 'Finance' in xxx__['Category Groups'] else \
('Biotechnology' if 'Biotechnology' in xxx__['Category Groups'] else \
('Health Care' if 'Health Care' in xxx__['Category Groups'] else \
('E-Commerce' if 'E-Commerce' in xxx__['Category Groups'] else \
('Software' if 'Software' in xxx__['Category Groups'] else \
('Internet' if 'Internet' in xxx__['Category Groups'] else \
('Information Technology' if 'Information Technology' in xxx__['Category Groups'] else \
('Education' if 'Education' in xxx__['Category Groups'] else \
('Security' if 'Security' in xxx__['Category Groups'] else \
('Education' if 'Education' in xxx__['Category Groups'] else \
('Real Estate' if 'Real Estate' in xxx__['Category Groups'] else \
('Tourism' if 'Tourism' in xxx__['Category Groups'] else \
('Artificial Intelligence' if 'Artificial Intelligence' in xxx__['Category Groups'] else \
('Food' if 'Food' in xxx__['Category Groups'] else \
('Advertising' if 'Advertising' in xxx__['Category Groups'] else \
('Fashion' if 'Fashion' in xxx__['Category Groups'] else \
('Data' if 'Data' in xxx__['Category Groups'] else \
('Robotics' if 'Robotics' in xxx__['Category Groups'] else \
('Gaming' if 'Gaming' in xxx__['Category Groups'] else \
('Sports' if 'Sports' in xxx__['Category Groups'] else \
('Entertainment' if 'Entertainment' in xxx__['Category Groups'] else \
('Insurance' if 'Insurance' in xxx__['Category Groups'] else \
('Unknown'))))))))))))))))))))))", return_type='str')
op45 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Finance")))
op11723 = InitTable("data_168.pickle")
op46 = Append([op11723,op45])
op325282 = InitTable("data_65.pickle")
op48 = Append([op46,op325282])
op49 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Health Care")))
op50 = Append([op48,op49])
op51 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Internet")))
op52 = Append([op50,op51])
op53 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Biotechnology")))
op54 = Append([op52,op53])
op55 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Artificial Intelligence")))
op56 = Append([op54,op55])
op57 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Information Technology")))
op58 = Append([op56,op57])
op59 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Education")))
op60 = Append([op58,op59])
op61 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Advertising")))
op62 = Append([op60,op61])
op63 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Data")))
op64 = Append([op62,op63])
op65 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Food")))
op66 = Append([op64,op65])
op67 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Real Estate")))
op68 = Append([op66,op67])
op69 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Security")))
op70 = Append([op68,op69])
op484397 = InitTable("data_217.pickle")
op72 = Append([op70,op484397])
op73 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Gaming")))
op74 = Append([op72,op73])
op75 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Robotics")))
op76 = Append([op74,op75])
op77 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Fashion")))
op78 = Append([op76,op77])
op79 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Sports")))
op80 = Append([op78,op79])
op81 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Tourism")))
op82 = Append([op80,op81])
op83 = Filter(op14, BinOp(Field("Categories"),'==',Constant("Insurance")))
op84 = Append([op82,op83])
op85 = Filter(op84, BinOp(Field("Headquarters Regions"),'==',Constant("European Union (EU)")))
op86 = Filter(op84, BinOp(Field("Headquarters Regions"),'==',Constant("San Francisco Bay Area, West Coast, Western US")))
op87 = Append([op85,op86])
op879153 = InitTable("data_181.pickle")
op89 = Append([op87,op879153])
op90 = Filter(op84, BinOp(Field("Headquarters Regions"),'==',Constant("Greater New York Area, East Coast, Northeastern US")))
op91 = Append([op89,op90])
op92 = Filter(op84, BinOp(Field("Headquarters Regions"),'==',Constant("Greater Los Angeles Area, West Coast, Western US")))
op93 = Append([op91,op92])

ops = [op2, op3, op5, op6, op12, op13, op14, op45, op11723, op46, op325282, op48, op49, op50, op51, op52, op53, op54, op55, op56, op57, op58, op59, op60, op61, op62, op63, op64, op65, op66, op67, op68, op69, op70, op484397, op72, op73, op74, op75, op76, op77, op78, op79, op80, op81, op82, op83, op84, op85, op86, op87, op879153, op89, op90, op91, op92, op93]

output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = get_output_filter(ops, './temp')

print(output_filter)
#print(output_filter)
#print(output_filter)
#exit(0)
for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = {None:output_filter}
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    inference_i = op_i.get_inference_instance(output_filter_i)
    inference_i.input_filters = generate_input_filters(op_i, inference_i, AllOr(*list(output_filter_i.values())))
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    #print(inference_i.output_filter)

check_pushdown_result(ops, 'temp/')
