from email.headerregistry import Group
import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

raw_crops_dataset = InitTable('FAOSTAT_data_crops_CHandNeighbours.csv')
raw_crops_dataset1 = DropColumns(raw_crops_dataset, [])
raw_crops_dataset2 = Filter(raw_crops_dataset1, Not(BinOp(Constant('Data not available'), 'subset', Field('Flag Description'))))

CH_imports = InitTable('FAOSTAT_data_11-23-2019.csv')
CH_imports1 = DropColumns(CH_imports, ['Domain', 'Flag', 'Domain Code', 'Partner Country Code', 'Item Code', 'Reporter Country Code', 'Element Code', 'Year Code'])
CH_exports = InitTable('FAOSTAT_data_exports.csv')
CH_exports1 = DropColumns(CH_exports, ['Domain', 'Flag', 'Domain Code', 'Partner Country Code', 'Item Code', 'Reporter Country Code', 'Element Code', 'Year Code'])
CH_trade = Append([CH_imports1, CH_exports1])
unofficial_stats_index = Filter(CH_trade, BinOp(Field('Flag Description'), '!=', Constant('Unofficial figure')))
CH_trade1 = Filter(unofficial_stats_index, BinOp(Field('Unit'), '==', Constant('tonnes')))
# We copy the CH_trade dataframe in CH_trade_network to have an unchanged version of the variable for future graph analysis
CH_trade2 = DropColumns(CH_trade1, ['Flag Description', 'Reporter Countries'])
CH_trade3 = GroupBy(CH_trade2, ['Item', 'Year', 'Element'], {'Value':'sum'}, {'Value':'Value'})
CH_trade_transformed = Pivot(CH_trade3, ['Item','Year'],'Element','Value',None,{'Export Quantity':'float', 'Import Quantity':'float'})
CH_trade_transformed1 = Rename(CH_trade_transformed, {'Export Quantity':'Exported Quantity','Import Quantity':'Imported Quantity'})
CH_trade4 = InnerJoin(CH_trade3, CH_trade_transformed1, ['Item', 'Year'], ['Item', 'Year'])
CH_trade5 = DropColumns(CH_trade4, ['Value', 'Element'])
CH_trade6 = GroupBy(CH_trade5, ['Item', 'Year'], {'Exported Quantity':'mean','Imported Quantity':'mean'}, {'Exported Quantity':'Exported Quantity','Imported Quantity':'Imported Quantity'})

raw_crops_dataset3 = Filter(raw_crops_dataset2, AllAnd(*[BinOp(Field('Area'), '==', Constant('Switzerland')), \
    BinOp(Field('Element'),'==',Constant('Production')), BinOp(Field('Year'), '>=', Constant(1986))]))
CH_data = InnerJoin(raw_crops_dataset3, CH_trade6, ['Item', 'Year'], ['Item', 'Year'])
CH_data2 = Rename(CH_data, {'Value':'Country production', 'Imported Quantity':'Importation', 'Exported Quantity':'Exportation'})
CH_data_transformed = UnPivot(CH_data2, ['Area', 'Element','Item','Year','Unit'], ['Country production', 'Importation'], var_name='Input', value_name='Value')
CH_restrained = Filter(CH_data_transformed, BinOp(Field('Item'), 'in', Constant(['Apples','Wheat','Potatoes', 'Maize','Sugar beet', 'Grapes', 'Barley'])))
