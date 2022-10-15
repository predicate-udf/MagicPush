
import sys
# from ppl_interface import *
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

# Yin: There is a bug in the code generating these operators.
# op4 is not the dependent operator of anyone.

op0 = InitTable("data_0.pickle", '.pickle')
op1 = Filter(op0, BinOp(Field("COUNTRY_OF_CITIZENSHIP"),'==',Constant("SOUTH KOREA")))
op3 = Filter(op1, BinOp(Field("WAGE_OFFER_UNIT_OF_PAY_9089"),'==',Constant("Year")))
op4 = DropColumns(op3, ["FW_INFO_EDUCATION_OTHER","DECISION_ELAPSED_YEARS","JOB_INFO_EDUCATION_OTHER","DECISION_DATE_MONTH","AGENT_FIRM_NAME","WAGE_OFFER_UNIT_OF_PAY_9089","FOREIGN_WORKER_INFO_CITY","DECISION_DATE","EMPLOYER_NAME","CASE_RECEIVED_DATE","JOB_INFO_JOB_TITLE","FW_INFO_POSTAL_CODE","AGENT_CITY","CASE_NUMBER","PW_SOC_CODE","Unnamed: 0","Unnamed: 1","FW_INFO_BIRTH_COUNTRY","AGENT_STATE","EMPLOYER_CITY","DECISION_DATE_YEAR","JOB_INFO_WORK_CITY","JOB_INFO_WORK_POSTAL_CODE","PW_SOC_TITLE","PW_AMOUNT_9089","JOB_INFO_EDUCATION","CASE_RECEIVED_DATE_YEAR","FOREIGN_WORKER_INFO_STATE","JOB_INFO_WORK_STATE"])
op5 = SetItem(op4, 'EMPLOYER_STATES', "lambda xxx__: {'AL': 'ALABAMA', 'AK': 'ALASKA', 'AZ': 'ARIZONA', 'AR': 'ARKANSAS', 'CA': 'CALIFORNIA', 'CO': 'COLORADO', 'CT': 'CONNECTICUT', 'DE': 'DELAWARE', 'FL': 'FLORIDA', 'GA': 'GEORGIA', 'HI': 'HAWAII', 'ID': 'IDAHO', 'IL': 'ILLINOIS', 'IN': 'INDIANA', 'IA': 'IOWA', 'KS': 'KANSAS', 'KY': 'KENTUCKY', 'LA': 'LOUISIANA', 'ME': 'MAINE', 'MD': 'MARYLAND', 'MA': 'MASSACHUSETTS', 'MI': 'MICHIGAN', 'MN': 'MINNESOTA', 'MS': 'MISSISSIPPI', 'MO': 'MISSOURI', 'MT': 'MONTANA', 'NE': 'NEBRASKA', 'NV': 'NEVADA', 'NH': 'NEW HAMPSHIRE', 'NJ': 'NEW JERSEY', 'NM': 'NEW MEXICO', 'NY': 'NEW YORK', 'NC': 'NORTH CAROLINA', 'ND': 'NORTH DAKOTA', 'OH': 'OHIO', 'OK': 'OKLAHOMA', 'OR': 'OREGON', 'PA': 'PENNSYLVANIA', 'RI': 'RHODE ISLAND', 'SC': 'SOUTH CAROLINA', 'SD': 'SOUTH DAKOTA', 'TN': 'TENNESSEE', 'TX': 'TEXAS', 'UT': 'UTAH', 'VT': 'VERMONT', 'VA': 'VIRGINIA', 'WA': 'WASHINGTON', 'WV': 'WEST VIRGINIA', 'WI': 'WISCONSIN', 'WY': 'WYOMING'}[xxx__['EMPLOYER_STATE']] \
    if xxx__['EMPLOYER_STATE'] in ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'] else xxx__['EMPLOYER_STATE']", 'str')
op8 = DropNA(op5, ["CASE_STATUS","DECISION_ELAPSED_DAYS","EMPLOYER_POSTAL_CODE","EMPLOYER_STATE","EMPLOYER_NUM_EMPLOYEES","WAGE_OFFER_FROM_9089","COUNTRY_OF_CITIZENSHIP","CLASS_OF_ADMISSION","FOREIGN_WORKER_INFO_EDUCATION","CASE_RECEIVED_DATE_MONTH","EMPLOYER_STATES"])
op32 = DropColumns(op8, ["CASE_STATUS"])

ops = [op0, op1, op3, op4, op5, op8, op32]

'''
op0 = InitTable("data_0.pickle", '.pickle')
op1 = Filter(op0, BinOp(Field("COUNTRY_OF_CITIZENSHIP"),'==',Constant("SOUTH KOREA")))
op3 = Filter(op1, BinOp(Field("WAGE_OFFER_UNIT_OF_PAY_9089"),'==',Constant("Year")))
op4 = SetItem(op3, 'EMPLOYER_STATES', (lambda xxx__: {'AL': 'ALABAMA', 'AK': 'ALASKA', 'AZ': 'ARIZONA', 'AR': 'ARKANSAS', 'CA': 'CALIFORNIA', 'CO': 'COLORADO', 'CT': 'CONNECTICUT', 'DE': 'DELAWARE', 'FL': 'FLORIDA', 'GA': 'GEORGIA', 'HI': 'HAWAII', 'ID': 'IDAHO', 'IL': 'ILLINOIS', 'IN': 'INDIANA', 'IA': 'IOWA', 'KS': 'KANSAS', 'KY': 'KENTUCKY', 'LA': 'LOUISIANA', 'ME': 'MAINE', 'MD': 'MARYLAND', 'MA': 'MASSACHUSETTS', 'MI': 'MICHIGAN', 'MN': 'MINNESOTA', 'MS': 'MISSISSIPPI', 'MO': 'MISSOURI', 'MT': 'MONTANA', 'NE': 'NEBRASKA', 'NV': 'NEVADA', 'NH': 'NEW HAMPSHIRE', 'NJ': 'NEW JERSEY', 'NM': 'NEW MEXICO', 'NY': 'NEW YORK', 'NC': 'NORTH CAROLINA', 'ND': 'NORTH DAKOTA', 'OH': 'OHIO', 'OK': 'OKLAHOMA', 'OR': 'OREGON', 'PA': 'PENNSYLVANIA', 'RI': 'RHODE ISLAND', 'SC': 'SOUTH CAROLINA', 'SD': 'SOUTH DAKOTA', 'TN': 'TENNESSEE', 'TX': 'TEXAS', 'UT': 'UTAH', 'VT': 'VERMONT', 'VA': 'VIRGINIA', 'WA': 'WASHINGTON', 'WV': 'WEST VIRGINIA', 'WI': 'WISCONSIN', 'WY': 'WYOMING'}[xxx__['EMPLOYER_STATE']] \
    if xxx__['EMPLOYER_STATE'] in ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'] else xxx__['EMPLOYER_STATE']))
op5 = DropColumns(op3, ["FW_INFO_EDUCATION_OTHER","DECISION_ELAPSED_YEARS","JOB_INFO_EDUCATION_OTHER","DECISION_DATE_MONTH","AGENT_FIRM_NAME","WAGE_OFFER_UNIT_OF_PAY_9089","FOREIGN_WORKER_INFO_CITY","DECISION_DATE","EMPLOYER_NAME","CASE_RECEIVED_DATE","JOB_INFO_JOB_TITLE","FW_INFO_POSTAL_CODE","AGENT_CITY","CASE_NUMBER","PW_SOC_CODE","Unnamed: 0","Unnamed: 1","FW_INFO_BIRTH_COUNTRY","AGENT_STATE","EMPLOYER_CITY","DECISION_DATE_YEAR","JOB_INFO_WORK_CITY","JOB_INFO_WORK_POSTAL_CODE","PW_SOC_TITLE","PW_AMOUNT_9089","JOB_INFO_EDUCATION","CASE_RECEIVED_DATE_YEAR","FOREIGN_WORKER_INFO_STATE","JOB_INFO_WORK_STATE"])
op8 = DropNA(op5, ["CASE_STATUS","DECISION_ELAPSED_DAYS","EMPLOYER_POSTAL_CODE","EMPLOYER_STATE","EMPLOYER_NUM_EMPLOYEES","WAGE_OFFER_FROM_9089","COUNTRY_OF_CITIZENSHIP","CLASS_OF_ADMISSION","FOREIGN_WORKER_INFO_EDUCATION","CASE_RECEIVED_DATE_MONTH","EMPLOYER_STATES"])
op32 = DropColumns(op8, ["CASE_STATUS"])
'''
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
    inference_i.input_filters = generate_input_filters(op_i, inference_i, AllOr(*list(output_filter_i.values())), output_schemas)
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    #print(inference_i.output_filter)

check_pushdown_result(ops, 'temp/')


# output_filter =  BinOp(Field('PaymentMethod'), '==', Constant(1))

# for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
#     if(op_i == ops[-1]):
#         output_filter_i = output_filter
#     else:
#         output_filter_i = generate_output_filter_from_previous(op_i, ops)
#     print(output_filter_i)
#     inference_i = op_i.get_inference_instance(output_filter_i)
#     inference_i.input_filters = generate_input_filters(op_i, inference_i, output_filter_i)
#     print(op_id+1, print_input_filters(inference_i))
