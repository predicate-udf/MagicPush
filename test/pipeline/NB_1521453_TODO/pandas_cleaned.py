import pandas as pd


food = pd.read_csv('discrim.csv')
food = food[~food.psoda.isnull() & ~food.pfries.isnull() & ~food.pentree.isnull()]
# drop the rows without prppov and prpblck, our targets of interest
food = food[~food.prppov.isnull() & ~food.prpblck.isnull()]
# for the missing values
def impute_medians(df, cols=[]):
    for col in cols:
        mval = df[col].median()
        df.loc[df[col].isnull(), col] = mval
    return df

food = food.groupby('chain').apply(impute_medians, cols=['wagest','nmgrs','nregs','emp'])
# for density and crmrte i will impute the median by county:
food = food.groupby('county').apply(impute_medians, cols=['density','crmrte'])
