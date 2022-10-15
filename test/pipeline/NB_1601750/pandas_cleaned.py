import pandas as pd

data = pd.read_csv('data.csv')
locations = pd.read_csv('locations.csv')
print(locations.columns)
data2 = data.set_index('uniqueid').join(locations.set_index('uniqueid')[['geographiclevel','stateabbr','cityname']]).copy().reset_index()
print(data2.columns)
data2 = data2[data2['geographiclevel'] == 'Census Tract']
data2 = data2.drop(['Unnamed: 0','datavaluetypeid','geographiclevel'],axis=1).reset_index()
#print(data2.columns)
#data2 = data2[data2["stateabbr"].isin(['NY','TX','CA','CT'])]
dataMelt = data2.melt(id_vars=['uniqueid','stateabbr','cityname','populationcount'],var_name='measureid')

filt1 = dataMelt['measureid']=='SLEEP'
filt2 = dataMelt['measureid']=='LPA'
filt3 = dataMelt['measureid']=='OBESITY'
filt4 = dataMelt['measureid']=='CHD'
filtered = dataMelt[(filt1)|(filt2)|(filt3)|(filt4)]
pivoted = filtered.reset_index().pivot_table('value',['uniqueid','populationcount','stateabbr','cityname'],'measureid').reset_index()
print(pivoted)
obesity = pivoted[pivoted["stateabbr"].isin(['NY','TX','CA','CT'])]
