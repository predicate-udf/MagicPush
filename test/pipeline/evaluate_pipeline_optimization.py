import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")

import z3
import dis
from interface import *
from util import *
import random
# from constraint import *
from predicate import *
from generate_input_filters import generate_input_filters_general, generate_output_filter_from_previous
from compare_pushdown_result import get_output_filter, check_pushdown_result, get_output_filter_all_operators
import os
from table_constraint import *
from input_filter_baseline import get_input_filter_baseline
from eval_util import *
import numpy as np



def is_a_filter(op):
    return isinstance(op, Filter) or isinstance(op, DropNA) # or isinstance(op, TopN)

# FilterInference
# InnerJoinInference, LeftOuterJoinInference
# GroupByInference -- 
# RenameInference
# PivotInference, GetDummiesInference
# SetItemInference, ChangeTypeInference
# SortValueInference, CopyInference, AppendInference, InitTableInference
# UnPivotInference
# SplitInference -- special lambda
# DropColumnsInference
# TopNInference -- nothing for now

NB_path = sys.argv[1]
pipe_code, orig_ops = read_pipeline_code(NB_path)
ops = []
pipe_code.append("ops = [{}]".format(','.join(orig_ops)))
print(''.join(pipe_code))
exec(''.join(pipe_code))

if not any([is_a_filter(op) for op in ops]):
    print("{} PIPELINE HAS NO FILTER".format(NB_path))
    exit(0)

output_filter = True
output_schemas = generate_output_schemas(ops)
superset = []
use_baseline = False
assertion=False
scale_data_only = False
if len(sys.argv) > 2 and 'base' in sys.argv[2]:
    use_baseline = True
    if len(sys.argv) > 3:
        assertion=True
if len(sys.argv) > 2 and 'scale' in sys.argv[2]:
    scale_data_only = True
    
    
import time
start = time.time()
for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = {None:output_filter}
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    output_filter = AllOr(*list(output_filter_i.values()))
    inference = op_i.get_inference_instance(output_filter)
    #print("output filter for {} is {}".format(type(inference), inference.output_filter))
    last_return = None
    if use_baseline:
        print(type(inference))
        inference.input_filters = get_input_filter_baseline(output_filter, op_i, inference, assertion)
        print(inference.input_filters[0])
        if all([type(p) is bool and p==True for p in inference.input_filters]):
            superset.append(op_i)
    else:
        last_filters = []
        while True:
            last_return, inference.input_filters = generate_input_filters_general(op_i, inference, output_filter, output_schemas, last_return)
            #print(last_return, inference.input_filters)
            #print("output filter of {} = {}".format(type(op_i), inference.output_filter))
            #print("input filter infered:")
            # for i in range(len(inference.input_filters)):
            #     print("---{}".format(inference.input_filters[i]))
            if any([all([str(history[i])==str(inference.input_filters[i]) for i in range(len(inference.input_filters))]) for history in last_filters]):
                inference.input_filters = [True for i in range(len(inference.input_filters))]
                #print("CANNOT PUSHDOWN")
                break
            last_filters.append(inference.input_filters)
            if inference.check_small_model() and inference.verify_correct():
                break
            if inference.check_small_model(check_superset=True) and inference.verify_correct(check_superset=True):
                superset.append(op_i)
                break


print("TIME = {}".format(time.time()-start))
is_superset = len(superset) > 0


# print("\n")
# for op in ops:
#     if isinstance(op, InitTable):
#         print("------------")
#         print("FILTER ON INPUT TABLE: {} {} {}".format(op.datafile_path, op.inference.input_filters[0], '/ superset ({})'.format(is_superset) if len(is_superset)>0 else ''))
#         simplify_input_filter(op.inference)
#         #evaluate_size(op)

from pipeline_opt_eval_codegen import *

path = '/datadrive/yin/pipeline_opt_experiment/{}'.format(list(filter(lambda x: len(x) > 0, NB_path.split('/')))[-1])
os.system("mkdir {}".format(path))
os.system("mkdir {}/data".format(path))
os.system('cp {} {}/'.format(os.path.join(NB_path, 'pandas_cleaned.py'), path))
os.system('cp {}/*.pickle {}/data/'.format(NB_path, path))
os.system('cp {}/*.csv {}/data/'.format(NB_path, path))
orig_code = read_original_code(os.path.join(NB_path, 'pandas_cleaned.py'), ops)
if use_baseline:

    # run baseline on original data, get time and data size
    # run baseline code on scaled data, get time
    for op in ops:
        if isinstance(op, InitTable):
            evaluate_size(op, get_scaled_data=True, path=path)
    code = generate_script_with_filter_on_input(orig_code, ops, orig_ops,  is_superset, path)
    with open(os.path.join(path, 'baseline_ppl.py'), 'w') as fp:
        fp.write(code)


elif scale_data_only:
    for op in ops:
        if isinstance(op, InitTable):
            evaluate_size(op, get_scaled_data=True, path=path)
else:
    # run original code on original data, get time
    # run original code on scaled data, get time
    code = generate_script_with_no_filter(orig_code, path)
    with open(os.path.join(path, 'orig_ppl.py'), 'w') as fp:
        fp.write(code)

    for op in ops:
        if isinstance(op, InitTable):
            evaluate_size(op)
    # run opt code on original data, get time and data size
    # run opt code on scaled data, get time
    code = generate_script_with_filter_on_input(orig_code, ops, orig_ops,  is_superset, path)
    with open(os.path.join(path, 'opt_ppl.py'), 'w') as fp:
        fp.write(code)

print("PIPELINE LENGTH = {}".format(len(ops)))




# FILTER ON INPUT TABLE: NB_140759/data_0.pickle (((True && ( { -1:0 }[xxx__["young"]] if xxx__["young"] in [-1] else  xxx__["young"] == c-0)) && True) || ((True && ( { -1:0 }[xxx__["young"]] if xxx__["young"] in [-1] else  xxx__["young"] == c-1)) && True)) 
# FILTER ON INPUT TABLE: NB_140759/data_1.pickle (((([partition] == c-0) && True) && True) || ((([partition] == c-0) && True) && True)) 
# baseline fails: not cnf when pushed beyond inner join

# NB_152739: *** an interesting case, verifier generates a much simpler predicate than baseline
# FILTER ON INPUT TABLE: NB_152739/data_0.pickle (([city_code] == c-c5) && True)
# baseline passes

# NB_1003349 PIPELINE HAS NO FILTER

# NB_1038750 PIPELINE HAS NO FILTER

# FILTER ON INPUT TABLE: NB_1038750/data_0.pickle ((([País/Região] == c-Afghanistan) && True) || (([País/Região] == c-Afghanistan) && True))
# baseline can be ok: pivot but filter on index col

# NB_1044027 PIPELINE HAS NO FILTER

# FILTER ON INPUT TABLE: NB_1064516/data_0.pickle (((isnotnull([body]) && True) || True) || (isnotnull([article]) && True)) 
# FILTER ON INPUT TABLE: NB_1064516/data_13.pickle (((isnotnull([body]) && True) || True) || (isnotnull([article]) && True)) 
# baseline passes

# NB_1089645 PIPELINE HAS NO FILTER

# FILTER ON INPUT TABLE: NB_1097750/data_9.pickle (isnotnull([Unnamed: 1]) && True)
# baseline passes

# NB_1492575 PIPELINE HAS NO FILTER

# NB_1502454: a lot of filters

# FILTER ON INPUT TABLE: NB_1595552/data_0.pickle ((((((((((isnotnull([Name]) && isnotnull([Platform])) && isnotnull([Year_of_Release])) && isnotnull([Genre])) && isnotnull([Publisher])) && isnotnull([NA_Sales])) && isnotnull([EU_Sales])) && isnotnull([JP_Sales])) && isnotnull([Other_Sales])) && isnotnull([Global_Sales])) && True) 
# baseline passes

# NB_3725235 PIPELINE HAS NO FILTER

# NB_3796021: a lot of filters

# NB_3843923
# baseline fails at unpivot
# FILTER ON INPUT TABLE: NB_3843923/data_22.pickle ((((((True && True) && True) && True) && True) && True) || (((((isnotnull([country]) && isnotnull([year])) && True) && True) && True) && True)) 

# NB_3897010 PIPELINE HAS NO FILTER

# NB_3951300
# FILTER ON INPUT TABLE: NB_3951300/data_0.pickle ((( xxx__["Country/Region"].lstrip() != c-Others) && ( xxx__["Country/Region"].lstrip() != c-Diamond Princess)) && (( xxx__["Country/Region"].lstrip() != c-MS Zaandam) && (( xxx__["Country/Region"].lstrip() != c-Kosovo) && (( xxx__["Country/Region"].lstrip() != c-Holy See) && (( xxx__["Country/Region"].lstrip() != c-Vatican City) && (( xxx__["Country/Region"].lstrip() != c-Timor-Leste) && (( xxx__["Country/Region"].lstrip() != c-East Timor) && (( xxx__["Country/Region"].lstrip() != c-Channel Islands) && (( xxx__["Country/Region"].lstrip() != c-Western Sahara) && True))))))))) 
# baseline passes

# NB_4001709
# FILTER ON INPUT TABLE: NB_4001709/data_0.pickle (([COUNTRY_OF_CITIZENSHIP] == c-SOUTH KOREA) && (([WAGE_OFFER_UNIT_OF_PAY_9089] == c-Year) && (((((((((((isnotnull([CASE_STATUS]) && isnotnull([DECISION_ELAPSED_DAYS])) && isnotnull([EMPLOYER_POSTAL_CODE])) && isnotnull([EMPLOYER_STATE])) && isnotnull([EMPLOYER_NUM_EMPLOYEES])) && isnotnull([WAGE_OFFER_FROM_9089])) && isnotnull([COUNTRY_OF_CITIZENSHIP])) && isnotnull([CLASS_OF_ADMISSION])) && isnotnull([FOREIGN_WORKER_INFO_EDUCATION])) && isnotnull([CASE_RECEIVED_DATE_MONTH])) && isnotnull( {'AL': 'ALABAMA', 'AK': 'ALASKA', 'AZ': 'ARIZONA', 'AR': 'ARKANSAS', 'CA': 'CALIFORNIA', 'CO': 'COLORADO', 'CT': 'CONNECTICUT', 'DE': 'DELAWARE', 'FL': 'FLORIDA', 'GA': 'GEORGIA', 'HI': 'HAWAII', 'ID': 'IDAHO', 'IL': 'ILLINOIS', 'IN': 'INDIANA', 'IA': 'IOWA', 'KS': 'KANSAS', 'KY': 'KENTUCKY', 'LA': 'LOUISIANA', 'ME': 'MAINE', 'MD': 'MARYLAND', 'MA': 'MASSACHUSETTS', 'MI': 'MICHIGAN', 'MN': 'MINNESOTA', 'MS': 'MISSISSIPPI', 'MO': 'MISSOURI', 'MT': 'MONTANA', 'NE': 'NEBRASKA', 'NV': 'NEVADA', 'NH': 'NEW HAMPSHIRE', 'NJ': 'NEW JERSEY', 'NM': 'NEW MEXICO', 'NY': 'NEW YORK', 'NC': 'NORTH CAROLINA', 'ND': 'NORTH DAKOTA', 'OH': 'OHIO', 'OK': 'OKLAHOMA', 'OR': 'OREGON', 'PA': 'PENNSYLVANIA', 'RI': 'RHODE ISLAND', 'SC': 'SOUTH CAROLINA', 'SD': 'SOUTH DAKOTA', 'TN': 'TENNESSEE', 'TX': 'TEXAS', 'UT': 'UTAH', 'VT': 'VERMONT', 'VA': 'VIRGINIA', 'WA': 'WASHINGTON', 'WV': 'WEST VIRGINIA', 'WI': 'WISCONSIN', 'WY': 'WYOMING'}[xxx__['EMPLOYER_STATE']]     if xxx__['EMPLOYER_STATE'] in ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'] else xxx__['EMPLOYER_STATE'])) && True))) 
# baseline passes

# NB_4011573 PIPELINE HAS NO FILTER

# NB_4264471 
# FILTER ON INPUT TABLE: NB_4264471/data_0.pickle (isnotnull([Unnamed: 0]) && True)
# baseline passes

# NB_4300602 
# FILTER ON INPUT TABLE: NB_4300602/data_0.pickle ((([hotel_id] == c-21) && True) || (([hotel_id] != c-21) && True))
# baseline passes

# NB_4337090 
# FILTER ON INPUT TABLE: NB_4337090/data_0.pickle (([Element] == c-TMAX) && True) 
# baseline passes

# NB_4401467  **** our algorithm is more restrictive 
# FILTER ON INPUT TABLE: NB_4401467/data_0.pickle (([language] == c-english) && (([country] == c-US) && ((( xxx__["published"][0:10] == c-2016-10-26) || ( xxx__["published"][0:10] == c-2016-10-27)) && (True && (not(isnull([title])) && ((isnotnull([text]) && True) && True)))))) / superset ([<class 'interface.DropDuplicate'>])
# baseline: FILTER ON INPUT TABLE: NB_4401467/data_0.pickle (((True && (( xxx__["published"][0:10] == c-2016-10-26) || ( xxx__["published"][0:10] == c-2016-10-27))) && ([country] == c-US)) && ([language] == c-english)) 

# NB_4427906
# FILTER ON INPUT TABLE: NB_4427906/data_1.pickle (( 0 if pd.isnull(x[col]) else x[col] > c-0) && True) 
# baseline passes

# NB_4473143
# FILTER ON INPUT TABLE: NB_4473143/data_0.pickle ((([Country] == c-South Sudan) && ([Year] <= c-2010)) && (isnotnull([Income Level]) && (([Year] == c-2015) && True))) 
# baseline passes

# NB_4482537
# FILTER ON INPUT TABLE: NB_4482537/data_0.pickle (not(isnull([language])) && True)
# baseline passes

# ***** TBD ******
# NB_4562158, not optimal, need to extract expr from lambda
# baseline passes but not optimal

# NB_8392403
# FILTER ON INPUT TABLE: NB_8392403/data_0.pickle ((isnull([date]) == c-False) && True) 
# FILTER ON INPUT TABLE: NB_8392403/data_7.pickle ((isnull([date]) == c-False) && True) 
# FILTER ON INPUT TABLE: NB_8392403/data_14.pickle ((isnull([date]) == c-False) && True) 
# FILTER ON INPUT TABLE: NB_8392403/data_21.pickle ((isnull([date]) == c-False) && True) 
# baseline passes

# new_pipes/NB_160505 PIPELINE HAS NO FILTER

# FILTER ON INPUT TABLE: new_pipes/NB_1053310/data_0.pickle ((((True && isnotnull([STRUCTURE_KIND_043A])) && ([STRUCTURE_LEN_MT_049] <= c-400)) && ([YEAR_BUILT_027] > c-1900)) && ([CULVERT_COND_062] == c-N)) 
# FILTER ON INPUT TABLE: new_pipes/NB_1053310/data_1.pickle (True && ([elevation] >= c-0)) 
# baseline passes

# new_pipes/NB_1089645 PIPELINE HAS NO FILTER

# new_pipes/NB_3759351
# long input filter
# baseline passes

# mew_pipes/NB_3766541 HAS NO FILTERS

# new_pipes/NB_3797619 PIPELINE HAS NO FILTER

# new_pipes/NB_3995742 
# long input filter
# baseline passes

# new_pipes/NB_4016709 pipeline needs to be fixed

# new_pipes/NB_4193289 PIPELINE HAS NO FILTER
# new_pipes/NB_4198708 PIPELINE HAS NO FILTER

# new_pipes/NB_4241794
# long input filter
# baseline passes

# new_pipes/NB_4573458 PIPELINE HAS NO FILTER

# new_pipes/NB_4580619 TBD
# replacing lambda with lambda
# baseline fails at split and unpivot

# new_pipes/NB_4611319 PIPELINE HAS NO FILTER

# new_pipes/NB_4643166 PIPELINE HAS NO FILTER


# new_pipes2/NB_154032
# FILTER ON INPUT TABLE: new_pipes2/NB_154032//FAOSTAT_data_crops_CHandNeighbours.csv ((c-Data not available subset [Flag Description]) && (((([Area] == c-Switzerland) && ([Element] == c-Production)) && ([Year] >= c-1986)) && (([Item] in c-['Apples', 'Wheat', 'Potatoes', 'Maize', 'Sugar beet', 'Grapes', 'Barley']) && True))) 
# FILTER ON INPUT TABLE: new_pipes2/NB_154032//FAOSTAT_data_11-23-2019.csv (([Flag Description] != c-Unofficial figure) && (([Unit] == c-tonnes) && ((([Item] in c-['Apples', 'Wheat', 'Potatoes', 'Maize', 'Sugar beet', 'Grapes', 'Barley']) && True) || (([Item] in c-['Apples', 'Wheat', 'Potatoes', 'Maize', 'Sugar beet', 'Grapes', 'Barley']) && True)))) 
# FILTER ON INPUT TABLE: new_pipes2/NB_154032//FAOSTAT_data_exports.csv (([Flag Description] != c-Unofficial figure) && (([Unit] == c-tonnes) && ((([Item] in c-['Apples', 'Wheat', 'Potatoes', 'Maize', 'Sugar beet', 'Grapes', 'Barley']) && True) || (([Item] in c-['Apples', 'Wheat', 'Potatoes', 'Maize', 'Sugar beet', 'Grapes', 'Barley']) && True)))) 
# baseline fails at unpivot

# ***** TBD ******
# new_pipes2/NB_1046688
# dropcolumn can have a predicate on pivot

# ***** TBD ******
# new_pipes2/NB_1568859
# unpivot, filter at value column can be pushed down
# baseline Fail to pushdown at <class 'interface.UnPivot'>

# new_pipes2/NB_1601750
# FILTER ON INPUT TABLE: new_pipes2/NB_1601750/data.csv (True && (True && (True && True))) 
# FILTER ON INPUT TABLE: new_pipes2/NB_1601750/locations.csv (([geographiclevel] == c-Census Tract) && (True && (([stateabbr] in c-['NY', 'TX', 'CA', 'CT']) && True))) 
# baseline Fail to pushdown at <class 'interface.Pivot'>

# ***** TBD ******
# new_pipes2/NB_3742233
# unpivot, filter at selected var column, [owner_move_in]==True or [breach]==True or [nuisance]==True
# baseline Fail to pushdown at <class 'interface.Pivot'>

# ***** TBD ******
# new_pipes2/NB_4294922, subpipeline to set a value
# baseline Fail to pushdown at <class 'interface.CrosstableUDF'>

# new_pipes/NB_4325788 -- subpipeline is not handled now
# FILTER ON INPUT TABLE: new_pipes2/NB_4325788/BX-Books.pickle (isnotnull([Book-Title]) && (True || (True && (True && True)))) / superset ([<class 'interface.LeftOuterJoin'>, <class 'interface.LeftOuterJoin'>])
# FILTER ON INPUT TABLE: new_pipes2/NB_4325788/BX-Users.pickle ((c-usa|canada subset [Location]) && True) / superset ([<class 'interface.LeftOuterJoin'>, <class 'interface.LeftOuterJoin'>])
# baseline fails at left outer join


######
# ***** TBD ******
# NB_1530967 TBD
# baseline fails

# NB_569068 TBD
# (([Series] == c-In 2018 constant prices at 2018 USD PPPs) && (((([Country] in c-['Greece', 'United Kingdom', 'United States', 'France', 'Germany', 'Poland']) && True) && True) && ((([Time] == c-2007) && True) || ((True && ([Time] >= c-2007)) && True)))
# baseline fails

# ***** TBD ******
# NB_1044976 TBD
# baseline fails

# NB_3822843 TBD
# (((True && (([DAY] == c-12) && True)) || (True && (([DAY] == c-12) && True))) && ((True && (True && True)) || (([IMPRESSIONS] > c-85007) && (True && True))))
# baseline fails


