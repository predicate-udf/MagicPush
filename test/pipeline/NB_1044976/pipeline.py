from multiprocessing import Pipe
import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

df_gdppp = InitTable('GDP_per_capita.pickle')
df_groups = InitTable('country_groups.pickle')

# implement with a UDF
sub_op0 = SubpipeInput(df_groups, 'table')
sub_op_row = SubpipeInput(df_gdppp, 'row')
filter_row = ScalarComputation({'row':sub_op_row}, "lambda row: row['Country Code']")
oecd_countries = Filter(sub_op0, \
    And(BinOp(Field('GroupCode'), '==', Constant('OED')), \
        BinOp(Field('CountryCode'), '==', filter_row)))
exists = AllAggregate(oecd_countries, Value(0), "lambda x,y: 1")
op1 = CrosstableUDF(df_gdppp, 'isin_country', \
    SubPipeline(PipelinePath([sub_op0, sub_op_row, filter_row, oecd_countries, exists])))
df_oecd_wide_extra_col = Filter(op1, BinOp(Field('isin_country'), '==', Constant(1)))
df_oecd_wide = DropColumns(df_oecd_wide_extra_col, ['isin_country'])

df_oecd = UnPivot(df_oecd_wide, ['Country Name', 'Country Code'], ['1960', '1961', '1962', '1963', '1964',\
       '1965', '1966', '1967', '1968', '1969', '1970', '1971', '1972', '1973',\
       '1974', '1975', '1976', '1977', '1978', '1979', '1980', '1981', '1982',\
       '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991',\
       '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000',\
       '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009',\
       '2010', '2011', '2012', '2013', '2014', '2015'], 'year','GDPPC')
df_oecd_1 = ChangeType(df_oecd, 'int', 'year', 'year')
df_oecd_2 = Filter(df_oecd_1, BinOp(Field('year'), ">=", Constant(1990)))

# df_oecd_normalized = df_oecd.groupby('Country Name').apply(normalize_to_2000)
# def normalize_to_2000(df):
#     ref = df.loc[df.year == 2000].iloc[0]['GDPPC']
#     df.GDPPC /= ref  
#     df.GDPPC = (df.GDPPC * 100) - 100
#     return df

sub_op0_1 = SubpipeInput(df_oecd_2, 'group', ['Country Name'])
sub_op1_1 = Filter(sub_op0_1, BinOp(Field('year'), '==', Constant(2000)))
sub_op2_1 = AllAggregate(sub_op1_1, Value(0, True), "lambda x,y: y['GDPPC']")
sub_op3_1 = SetItemWithDependency(sub_op0_1, 'GDPPC', {'x':sub_op0_1, 'ref':sub_op2_1}, 'lambda x,ref:(x["GDPPC"]/ref)*100-100')
df_oecd_normalized = CogroupedMap(SubPipeline([PipelinePath([sub_op0_1, sub_op1_1, sub_op2_1, sub_op3_1])]))
df_oecd_normalized1 = Filter(df_oecd_normalized, BinOp(Field('year'), '>=', Constant(2000)))
df_oecd_normalized2 = Filter(df_oecd_normalized1, BinOp(Field('Country Code'), '==', Constant('ISR')))

ops = [df_gdppp, df_groups, op1, df_oecd_wide_extra_col, df_oecd_wide, df_oecd, df_oecd_1, df_oecd_2, df_oecd_normalized, df_oecd_normalized1, df_oecd_normalized2]