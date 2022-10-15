import pandas as pd
import pickle

import requests

states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Northern Mariana Islands', 'Ohio', 'Oklahoma', 'Oregon', 'Palau', 'Pennsylvania', 'Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virgin Islands', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming', 'Guam']
us_state_abbrev = {'Alabama':'AL','Alaska':'AK','Arizona':'AZ','Arkansas':'AR','California':'CA','Colorado':'CO','Connecticut':'CT','Delaware':'DE','District of Columbia':'DC','Florida':'FL','Georgia':'GA','Hawaii':'HI','Idaho':'ID','Illinois':'IL','Indiana':'IN','Iowa':'IA','Kansas':'KS','Kentucky':'KY','Louisiana':'LA','Maine':'ME','Maryland':'MD','Massachusetts':'MA','Michigan':'MI','Minnesota':'MN','Mississippi':'MS','Missouri':'MO','Montana':'MT','Nebraska':'NE','Nevada':'NV','New Hampshire':'NH','New Jersey':'NJ','New Mexico':'NM','New York':'NY','North Carolina':'NC','North Dakota':'ND','Northern Mariana Islands':'MP','Ohio':'OH','Oklahoma':'OK','Oregon':'OR','Palau':'PW','Pennsylvania':'PA','Puerto Rico':'PR','Rhode Island':'RI','South Carolina':'SC','South Dakota':'SD','Tennessee':'TN','Texas':'TX','Utah':'UT','Vermont':'VT','Virgin Islands':'VI','Virginia':'VA','Washington':'WA','West Virginia':'WV','Wisconsin':'WI','Wyoming':'WY','Guam':'GM'}

ventilatorsPerState = pickle.load(open('ventilators.pickle', 'rb'))
ventilatorsPerState['State'] = ventilatorsPerState['State'].apply(lambda x: us_state_abbrev[x])
hospitalProfiles = pickle.load(open('hospitals_beds.pickle', 'rb'))
bedsPerState = hospitalProfiles.groupby(['State']).sum().reset_index()
hospitalResources = ventilatorsPerState.join(bedsPerState.set_index('State'), on='State')
hospitalResources['Ventilators/Bed'] = hospitalResources.apply(lambda x: x['Ventilators']/x['Beds'], axis=1)
stateStats = pickle.load(open('stateStats.pickle','rb'))
perCapitaResources = hospitalResources.join(stateStats, on='State')
perCapitaResources['Beds/100,000'] = perCapitaResources.apply(lambda x: x['Beds']/x['Population']*100000, axis = 1)
perCapitaResources['Ventilators/100,000'] = perCapitaResources.apply(lambda x: x['Ventilators']/x['Population']*100000, axis = 1)

def calcVentilators(row):
    beds = float(row['Beds'])
    state = row['State']
    ventilators = 0.0
    try:
        vpb = float(perCapitaResources[perCapitaResources['State'] == state]['Ventilators/Bed'])
        ventilators = beds * vpb
    except:
        pass        
    return ventilators
def vpbLookup(row):
    state = row['State']
    vpb = 0.0
    try:
        vpb = float(perCapitaResources[perCapitaResources['State'] == state]['Ventilators/Bed'])
    except:
        pass
    return vpb

hospitalProfiles['Estimated Ventilators'] = hospitalProfiles.apply(calcVentilators, axis=1)
hospitalProfiles['Statewide Ventilators per Bed'] = hospitalProfiles.apply(vpbLookup, axis=1)