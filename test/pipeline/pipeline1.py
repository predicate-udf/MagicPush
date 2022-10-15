
import sys
sys.path.append("../../")
sys.path.append("../")
import z3
import dis
from test_helper import *
from pandas_op import *
from util import *
import random
#from infer_schema import *
from interface import *
from predicate import *
from generate_input_filters import *


# NB 4514675
# https://github.com/nvyap/Restaurant-Location-Prediction/blob/bc869397f6535314e4e5d913929fe3a919790a59/src/python/Attributes%20Prediction/LocationPrediction-Copy2.ipynb

# raw_data = read_csv()
# dframe = raw_data[(raw_data['ffall']<888)&(raw_data['ffall']>=3)]
# dframe['totalStars'] =
# dframe['adjwhp'] = 
# zip_avg_ffall_revC_df = dframe.groupby(['zipcode']).agg([np.mean])['','','']
# zip_avg_ffall_revC_df.drop_duplicates(inplace=True)
# pop_data = pd.read_csv
# selected_pop = pop_data[['zipcode', 'PCT0050002', 'PCT0050003', 'PCT0050004', 'PCT0050005', 'PCT0050006',
    #    'PCT0050007', 'PCT0050008', 'PCT0050009', 'PCT0050010', 'PCT0050011',
    #    'PCT0050012', 'PCT0050013', 'PCT0050014', 'PCT0050015', 'PCT0050016',
    #    'PCT0050017', 'PCT0050018', 'PCT0050019', 'PCT0050020', 'PCT0050021',
    #    'PCT0050022']]
# joined_data = dframe.merge(selected_pop, left_on="zipcode", right_on="zipcode", how="inner", suffixes = ("_a","_b"))
# final_data = joined_data.merge(zip_avg_ffall_revC_df, left_on="zipcode", right_on="zipcode", how="inner", suffixes = ("_a","_b"))
# final_data['stars_avgffc'] = final_data['stars'] * final_data['avgffall'] 

import numpy as np
import random
import pickle

"""
# filter on output, save to pipeline_data/r1.p, pipeline_data/filter.p
raw_data = pd.read_csv('pipeline1_data/phoenix_business_ws_rw_ffall_merged2.csv', skipinitialspace=True)
pop_data = pd.read_csv('pipeline1_data/arizon.csv', skipinitialspace=True)
selected_pop = pop_data[['zipcode', 'PCT0050002', 'PCT0050003', 'PCT0050004', 'PCT0050005', 'PCT0050006',
       'PCT0050007', 'PCT0050008', 'PCT0050009', 'PCT0050010', 'PCT0050011',
       'PCT0050012', 'PCT0050013', 'PCT0050014', 'PCT0050015', 'PCT0050016',
       'PCT0050017', 'PCT0050018', 'PCT0050019', 'PCT0050020', 'PCT0050021',
       'PCT0050022']]
dframe = raw_data[(raw_data['ffall']<=888) & (raw_data['ffall']>=3)]
dframe['totalStars'] = dframe['review_count'] * dframe['stars']
dframe['adjwhp'] = dframe['white_pop'] * dframe['stars']
zip_avg_ffall_revC_df = dframe.groupby(['zipcode']).agg([np.mean])[['review_count', 'ffall','ffall_category']]
zip_avg_ffall_revC_df.columns = ['avgrc','avgffall','avgffc']
zip_avg_ffall_revC_df.drop_duplicates(inplace=True)

joined_data = dframe.merge(selected_pop, left_on="zipcode", right_on="zipcode", how="inner", suffixes = ("_a","_b"))
final_data = joined_data.merge(zip_avg_ffall_revC_df, left_on="zipcode", right_on="zipcode", how="inner", suffixes = ("_a","_b"))
final_data['stars_avgffc'] = final_data['stars'] * final_data['avgffall']
pos = random.randint(0, final_data.shape[0])
target_row = final_data.iloc[pos]
print(target_row)
values = {c:target_row[c] for c in final_data.columns}
output = final_data[final_data.apply(lambda x: all([x[c]==v for c,v in values.items()]), axis=1)]
pickle.dump(values, open('pipeline1_data/filter.p','wb'))
pickle.dump(output, open('pipeline1_data/r1.p','wb'))
exit(0)
"""

op0 = InitTable('pipeline1_data/phoenix_business_ws_rw_ffall_merged2.p')
op1 = InitTable('pipeline1_data/arizon.p')
op2 = Filter(op0, And(BinOp(Field('ffall'), '<', Constant(888)), (BinOp(Field('ffall'), '>=', Constant(3)))))
op3 = SetItem(op2, 'totalStars', 'lambda x: x[\'review_count\']*x[\'stars\']')
#op3 = SetItem(op2, 'totalStars', lambda x: x['review_count']*x['stars'])
#op4 = SetItem(op3, 'adjwhp', lambda x: x['white_pop']*x['stars'])
op4 = SetItem(op3, 'adjwhp', 'lambda x: x[\'white_pop\']*x[\'stars\']')
op5 = GroupBy(op4, ['zipcode'], {col:(Value(0,True), 'mean') for col in ['review_count', 'ffall','ffall_category']}, {'review_count':'avgrc','ffall':'avgffall','ffall_category':'avgffc'})
op6 = DropDuplicate(op5, ['zipcode','avgrc','avgffall','avgffc'])
op7 = InnerJoin(op4, op1, 'zipcode', 'zipcode')
op8 = InnerJoin(op6, op7, 'zipcode', 'zipcode')



ops = [op0, op1, op2, op3, op4, op5, op6, op7, op8]
output_schemas = generate_output_schemas(ops)

#eval = eval(generate_ops())


from compare_pushdown_result import get_output_filter, check_pushdown_result

output_filter = BinOp(Field('totalStars'), '==', Constant(1))

#output_filter = And(BinOp(Field('totalStars'), '>', Constant(609)),BinOp(Field('Burgers'), '==', Constant(0)) )
#output_filter = Or(BinOp(Field('totalStars'), '>', Constant(609)),BinOp(Field('Burgers'), '==', Constant(0)) )
output_filter = get_output_filter(ops, './temp')
print(output_filter)
#print(output_filter)
#print(output_filter)
#exit(0)
# for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
#     if(op_i == ops[-1]):
#         output_filter_i = {None:output_filter}
#     else:
#         output_filter_i = generate_output_filter_from_previous(op_i, ops)
#     inference_i = op_i.get_inference_instance(output_filter_i)
#     inference_i.input_filters = generate_input_filters_general(op_i, inference_i, AllOr(*list(output_filter_i.values())))
#     # print(output_filter_i)
#     print(op_id, ':')
#     print_input_filters(inference_i)
#     #print(inference_i.output_filter)



for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = {None:output_filter}
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    output_filter = AllOr(*list(output_filter_i.values()))
    inference_i = op_i.get_inference_instance(output_filter)
    last_return = None
    while True:
        last_return, inference_i.input_filters = generate_input_filters_general(op_i, inference_i, output_filter, output_schemas, last_return)
        if inference_i.check_small_model() and inference_i.verify_correct():
            break
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    print(type(op_i))



check_pushdown_result(ops, 'temp/')