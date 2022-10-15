import pandas as pd
import pickle

bundas_training = pd.read_csv('bundas_train.csv') # training set
bundas_testing = pd.read_csv('bundas_test.csv') # testing set


data = pd.concat([bundas_training,bundas_testing], ignore_index = True)
#item_avg_weight = data.pivot_table(values='Weight', index='Item_ID')
item_avg_weight = data.groupby('Item_ID')['Weight'].agg('mean').reset_index().set_index('Item_ID')
def impute_weight(cols):
    Weight = cols[0]
    Identifier = cols[1]
    if pd.isnull(Weight):
        return item_avg_weight['Weight'][item_avg_weight.index == Identifier]
    else:
        return Weight
data['Weight'] = data[['Weight','Item_ID']].apply(impute_weight,axis=1).astype(float)
#store_size_mode = data.pivot_table(values='Store_Size', columns='Store_Type',aggfunc=lambda y:y.mode())
store_size_mode = data.groupby('Store_Type')['Store_Size'].agg(lambda y:y.mode()).reset_index() # mode: most frequent value, TODO
#pickle.dump(store_size_mode, open('store_size_mode.pickle','wb'))
def impute_size_mode(columns):
    Size = columns[0]
    Type = columns[1]
    if pd.isnull(Size):
        #return store_size_mode.loc['Store_Size'][store_size_mode.columns == Type][0]
        return store_size_mode[store_size_mode['Store_Type']==Type]['Store_Size']
    else:
        return Size
data['Store_Size'] = data[['Store_Size','Store_Type']].apply(impute_size_mode,axis=1)
#visibility_item_avg = data.pivot_table(values='Visibility', index='Item_ID')
visibility_item_avg = data.groupby('Item_ID')['Visibility'].agg('mean').reset_index().set_index('Item_ID')
def impute_visibility_mean(cols):
    visibility = cols[0]
    item = cols[1]
    if visibility == 0:
        return visibility_item_avg['Visibility'][visibility_item_avg.index == item]
    else:
        return visibility
data['Visibility'] = data[['Visibility','Item_ID']].apply(impute_visibility_mean,axis=1).astype(float)
data['FatContent'].replace(['reg','low fat','LF'], ['Regular','Low Fat','Low Fat'], inplace=True)
data['Item_Category']=data['Item_ID'].apply(lambda x: x[0:2])
print(data.columns)