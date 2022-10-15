import pandas as pd

raw_crops_dataset = pd.read_csv('FAOSTAT_data_crops_CHandNeighbours.csv')
raw_crops_dataset =raw_crops_dataset[['Domain', 'Area', 'Element', 'Item', 'Year', 'Unit', 'Value', 'Flag Description']]

raw_crops_dataset.drop(index=raw_crops_dataset[raw_crops_dataset['Flag Description'].str.contains('Data not available')].index, inplace=True)

CH_imports = pd.read_csv('FAOSTAT_data_11-23-2019.csv')
CH_imports = CH_imports[['Reporter Countries', 'Partner Countries','Element','Item','Year','Unit','Value','Flag Description']]
CH_exports = pd.read_csv('FAOSTAT_data_exports.csv')
CH_exports = CH_exports[['Reporter Countries', 'Partner Countries','Element','Item','Year','Unit','Value','Flag Description']]

CH_trade = pd.concat([CH_imports, CH_exports])
unofficial_stats_index = CH_trade.loc[CH_trade['Flag Description']=='Unofficial figure'].index
CH_trade = CH_trade.drop(index = unofficial_stats_index)
CH_trade = CH_trade.loc[CH_trade.Unit=='tonnes']
# We copy the CH_trade dataframe in CH_trade_network to have an unchanged version of the variable for future graph analysis

CH_trade_network=CH_trade.copy()

CH_trade = CH_trade[['Element','Partner Countries', 'Item', 'Year', 'Unit', 'Value']]
CH_trade = CH_trade.groupby(['Item', 'Year', 'Element']).agg({'Value':'sum'}).reset_index()
CH_trade_transformed = pd.pivot(CH_trade,index=['Item', 'Year'], columns = 'Element', values='Value').rename(columns={'Export Quantity':'Exported Quantity','Import Quantity':'Imported Quantity'}).reset_index()

#CH_trade = pd.concat([CH_trade, CH_trade_transformed], axis=1, join='inner')
CH_trade = CH_trade.merge(CH_trade_transformed)
CH_trade.drop(columns=['Value', 'Element'], inplace=True)
CH_trade = CH_trade.groupby(['Item', 'Year']).agg({'Exported Quantity':'mean','Imported Quantity':'mean'}).reset_index()
print(CH_trade.shape)
CH_data = raw_crops_dataset.loc[raw_crops_dataset.Area=='Switzerland'].loc[raw_crops_dataset.Element=='Production'].loc[raw_crops_dataset.Year>= 1986]\
                                    .merge(CH_trade,on=['Item', 'Year'], how='inner')\
                                    .rename(columns={'Value':'Produced Quantity'})
print(CH_data.shape)
CH_data2 = CH_data.copy().rename(columns={'Produced Quantity':'Country production', 'Imported Quantity':'Importation', 'Exported Quantity':'Exportation'})

CH_data_transformed = pd.melt(CH_data2, value_vars=['Country production', 'Importation'], id_vars=['Area', 'Element','Item','Year','Unit'], var_name='Input', value_name='Value')
print(CH_data_transformed.shape)
CH_restrained = CH_data_transformed.loc[CH_data_transformed.Item.isin(['Apples','Wheat','Potatoes', 'Maize','Sugar beet', 'Grapes', 'Barley'])]
print(CH_restrained)