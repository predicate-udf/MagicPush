NB_246099.py: stats_category /              .groupby(['Monitoring_Period', 'Season']).apply(stats_category, 'Category_VOC')

            .groupby(['Monitoring_Period', 'Season']).apply(stats_category, 'Category_VOC')
def stats_category(df, cat_name):
    """
    This f-n calculates time distribution of indoor air parameters in comfort categories 
    and percentage of missing data.
    """
    if cat_name in ('Category_TEMP', 'Category_RH'):
        df_cat = pd.DataFrame(0, index=[cat_name], columns=labels_T_RH + ['Missing data'])
        for cat in labels_T_RH:
            df_cat.loc[cat_name, cat] = df[cat_name].isin([cat]).sum() * 100 / len(df[cat_name])
    else:
        df_cat = pd.DataFrame(0, index=[cat_name], columns=labels_CO2_VOC + ['Missing data'])
        for cat in labels_CO2_VOC:
            df_cat.loc[cat_name, cat] = df[cat_name].isin([cat]).sum() * 100 / len(df[cat_name])    
    df_cat.loc[cat_name, 'Missing data']  = df[cat_name].isna().sum() * 100 / len(df[cat_name])
    return df_cat.fillna(0)
    
# pred: df[cat] < some_value
# can get a superset? sum(filtered) / len(all) < sum(filtered) / len(filtered)