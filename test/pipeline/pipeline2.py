import sys
sys.path.append("../../")
sys.path.append("../")
import z3
import dis
from test_helper import *
from interface import *
from util import *
import random
from constraint import *
from predicate import *
from generate_input_filters import *

# NB 1534787
# https://github.com/smarty1palak/Customer_Churn_Prediction/blob/90151e5092081271ec5f8df483a38093bcf955de/Data_Analysis.ipynb

# telcom= pd.read_csv("WA_Fn-UseC_-Telco-Customer-Churn.csv")
# telcom = telcom.dropNA('TotalCharges") op1
# telcom['Churn'].replace(to_replace='Yes', value=1, inplace=True) op2
# telcom['Churn'].replace(to_replace='No',  value=0, inplace=True) op3
# replace_cols = [ 'OnlineSecurity', 'OnlineBackup', 'DeviceProtection',
#                 'TechSupport','StreamingTV', 'StreamingMovies']
# for i in replace_cols : 
#     telcom[i]  = telcom[i].replace({'No internet service' : 'No'}) op4,5,6,7,8,9
# telcom["SeniorCitizen"] = telcom["SeniorCitizen"].replace({1:"Yes",0:"No"}) op10, op11
# telcom = telcom.iloc[:,1:]  op11 drop column
# df = pd.get_dummies(telcom2) # df: output1 op12 get dummy
# df1 = telcom[(telcom.InternetService != "No") & (telcom.Churn == 1)]  op13
# df1 = pd.melt(df1[cols]).rename({'value': 'Has service'}, axis=1) # df1: output2


# data = pd.read_csv("./pipeline2_data/WA_Fn-UseC_-Telco-Customer-Churn.csv")
# schema = get_schema_from_df(data)


op0 = InitTable("./pipeline2_data/WA_Fn-UseC_-Telco-Customer-Churn.csv", '.csv')
op1 = DropNA(op0, ['TotalCharges'])
op2 = SetItem(op1, 'Churn', lambda x: x.replace('Yes', '1'))
op3 = SetItem(op2, 'Churn', lambda x: x.replace('No', '0'))
op4 = SetItem(op3, 'OnlineSecurity', lambda x: x.replace('No internet service', 'No'))
op5 = SetItem(op4, 'OnlineBackup', lambda x: x.replace('No internet service', 'No'))
op6 = SetItem(op5, 'DeviceProtection', lambda x: x.replace('No internet service', 'No'))
op7 = SetItem(op6, 'TechSupport', lambda x: x.replace('No internet service', 'No'))
op8 = SetItem(op7, 'StreamingTV', lambda x: x.replace('No internet service', 'No'))
op9 = SetItem(op8, 'StreamingMovies', lambda x: x.replace('No internet service', 'No'))
op10 = SetItem(op9, 'SeniorCitizen', lambda x: x.replace('1', 'Yes'))
op11 = SetItem(op10, 'SeniorCitizen', lambda x: x.replace('0', 'No'))

# now we only suuport int?

# op11 = DropColumns()
# op11.dependent_ops = [None]
# op12 = GetDummies() 
# op12.dependent_ops = [None]
# op13 = Filter()
# op13.dependent_ops = [None]





ops = [op1, op2,op3,op4,op5,op6,op7,op8,op9,op10, op11]

output_schemas = generate_output_schemas(ops)
output_filter =  BinOp(Field('PaymentMethod'), '==', Constant(1))

for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = output_filter
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    inference_i = op_i.get_inference_instance(output_filter_i)
    inference_i.input_filters = generate_input_filters(op_i, inference_i, output_filter_i)
    print(op_id+1, inference_i.input_filters[0])

