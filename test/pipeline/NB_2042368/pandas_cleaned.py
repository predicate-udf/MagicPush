import pandas as pd
orders=pd.read_csv('orders.csv')
train=pd.read_csv('order_products__train.csv')

otrain = orders[orders.eval_set=='train']
print(otrain.columns)
print(train.columns)
Alltrain = otrain.merge(train)
print(Alltrain.columns)
ytrain=Alltrain.loc[:,['user_id', 'product_id']]

ytrain.columns=['user_id', 'product_id_latest_train']

def products_concat(series):
    out = ''
    for product in series:
        if product > 0:
            out = out + str(int(product)) + ' '
    if out != '':
        return out.rstrip()
    else:
        return 'None'
# this creates a DataFrame of predicted latest order product list 

train_order = ytrain.groupby('user_id')["product_id_latest_train"].apply(products_concat).reset_index()
