import pandas as pd

sales=pd.read_csv("sales.csv", names=["saleId", "saleDateTime", "accountName", "coins", "currency", "priceInCurrency", "priceInEUR", "methodId", "ip", "ipCountry"], parse_dates=["saleDateTime"])
#Sort original csv file dates
sales = sales.sort_values(["saleDateTime"])
split1 = sales['saleDateTime'] < '2014-01-01 00:00:00'
sales14 = sales[split1]
split2 = sales['saleDateTime'] > '2013-12-31 23:59:59'
sales15 = sales[split2]
#Define function that returns boolean that shows if frequenty of purchases goes up

def func(x):
    q = True
    if x.iloc[0]['First'] < x.iloc[0]['Last']:
        q = False
    return q
#Favorite Countries:

customers = pd.DataFrame()

#First feature

customers['countries'] = sales14.groupby("accountName")["ipCountry"].nunique()
#Second feature

customers['lifespan'] = sales14.groupby("accountName")["saleDateTime"].max() - sales14.groupby("accountName")["saleDateTime"].min()

customers['lifespan'] = customers["lifespan"].dt.total_seconds()

#Third feature
customers['lifetime_spend'] = sales14.groupby("accountName")["priceInEUR"].sum()
#Fourth feature
customers['lifetime_trans'] = sales14.groupby("accountName")["saleId"].nunique()
#Fifth feature
tempx = sales14.groupby("accountName")["ipCountry"].value_counts()
temp5 = tempx.groupby("accountName").idxmax()
fav_countries = ["FR","DE","PL","GP","NL"]
customers['fav_country'] = pd.Series({item[0]: item[1] for item in temp5}).isin(fav_countries)
customers['fav_country'] = customers['fav_country'].fillna(0)
customers['fav_country'] = customers['fav_country'].astype(int)
#Sixth feature
tempy = sales14.groupby("accountName")["methodId"].value_counts()
temp6 = tempy.groupby("accountName").idxmax()
fav_paymethods = [2000,1000,40]
customers['fav_paymethod'] = pd.Series({item[0]: item[1] for item in temp6}).isin(fav_paymethods)
customers['fav_paymethod'] = customers['fav_paymethod'].fillna(0)
customers['fav_paymethod'] = customers['fav_paymethod'].astype(int)
#Seventh feature
first = sales14.groupby("accountName")["saleDateTime"].nsmallest(2)
last = sales14.groupby("accountName")["saleDateTime"].nlargest(2)

df = pd.DataFrame()
df['First'] = abs(first.diff().reset_index().groupby("accountName").tail(1).set_index("accountName")["saleDateTime"].dt.days)
df['Last'] = abs(last.diff().reset_index().groupby("accountName").tail(1).set_index("accountName")["saleDateTime"].dt.days)
print(df)
exit(0)

customers["freq_growth"] = df.groupby("accountName").apply(func)
customers["freq_growth"] = customers["freq_growth"].astype(int)
#Eighth feature

trans_growth = {}

for account, group in sales14.groupby('accountName')["priceInEUR"]:

    if group.shape[0] >= 2:

        trans_growth[account] = group.head(2).sum() < group.tail(2).sum()

customers["trans_growth"] = pd.Series(trans_growth)



customers["trans_growth"] = customers["trans_growth"].fillna(0)

customers["trans_growth"] = customers["trans_growth"].astype(int)
#Class feature: Does the customer return, set all to false

loyal = {}

customers["class"] = True

for account, group in customers.groupby('accountName')["class"]:

    if (account in sales15["accountName"].values):

        loyal[account] = True

    else:

        loyal[account] = False

customers["class"] = pd.Series(loyal)

customers["class"] = customers["class"].astype(int)