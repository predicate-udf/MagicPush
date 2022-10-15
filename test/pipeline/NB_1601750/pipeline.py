import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

data = InitTable('data.csv')
locations = InitTable('locations.csv')
locations1 = DropColumns(locations, ['Unnamed: 0', 'tractfips', 'cityfips', 'statedesc', 'latitude', 'longitude'])
data0 = InnerJoin(data, locations1, ['uniqueid'],['uniqueid'])
data1 = Filter(data0, BinOp(Field('geographiclevel'),'==', Constant('Census Tract')))
data2 = DropColumns(data1, ['Unnamed: 0','datavaluetypeid','geographiclevel'])

dataMelt = UnPivot(data2, ['uniqueid','stateabbr','cityname','populationcount'], \
    ['ACCESS2', 'ARTHRITIS', 'BINGE', 'BPHIGH', 'BPMED', 'CANCER', 'CASTHMA',\
       'CHD', 'CHECKUP', 'CHOLSCREEN', 'COLON_SCREEN', 'COPD', 'COREM',\
       'COREW', 'CSMOKING', 'DENTAL', 'DIABETES', 'HIGHCHOL', 'KIDNEY', 'LPA',\
       'MAMMOUSE', 'MHLTH', 'OBESITY', 'PAPTEST', 'PHLTH', 'SLEEP', 'STROKE',\
       'TEETHLOST'], var_name='measureid')
filtered = Filter(dataMelt, BinOp(Field('measureid'), 'in', Constant(['SLEEP','LPA','OBESITY','CHD'])))
pivoted = Pivot(filtered, ['uniqueid','populationcount','stateabbr','cityname'], 'measureid', 'value',None, \
    {'SLEEP':'float','LPA':'float','OBESITY':'float','CHD':'float'})
obesity = Filter(pivoted, BinOp(Field('stateabbr'), 'in', Constant(['NY','TX','CA','CT'])))
