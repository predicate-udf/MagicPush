import pandas as pd
import re
theCities = ['Batavia', 'Farmington', 'Denison', 'Atkins', 'Elgin', 'Williamsburg', 'Le_Claire', 'Radcliffe', 'Hopkinton', 'Webster_City', 'Altoona', 'University_Park', 'Battle_Creek', 'Ainsworth', 'Villisca', 'Colo', 'Bayard', 'Donnellson', 'Sheffield', 'Albert_City', 'Hartford', 'Mount_Ayr', 'Epworth', 'Humboldt', 'Le_Mars', 'Albion', 'Dumont', 'St_Charles', 'Ventura', 'Bancroft', 'Olin', 'Fruitland', 'St_Ansgar', 'Sibley', 'Bedford', 'Grand_Mound', 'Hartley', 'George', 'Missouri_Valley', 'Graettinger', 'What_Cheer', 'Albia', 'Hospers', 'Center_Point', 'Clarence', 'Estherville', 'Pomeroy', 'Monroe', 'Gilbertville', 'Gowrie', 'Kanawha', 'Quasqueton', 'Lamoni', 'Murray', 'Sac_City', 'Moulton', 'Mapleton', 'Montezuma', 'Winfield', 'Correctionville', 'Pocahontas', 'Nashua', 'Eagle_Grove', 'Columbus_Junction', 'Audubon', 'Traer', 'Cherokee', 'Hamburg', 'Waverly', 'Mediapolis', 'Pleasantville', 'Glidden', 'Bloomfield', 'Anita', 'Nora_Springs', 'Elma', 'Agency', 'Elk_Horn', 'Jefferson', 'Pacific_Junction', 'Lake_Park', 'Wellsburg', 'Chariton', 'Clarinda', 'Allerton', 'Madrid', 'Garnavillo', 'Buffalo_Center', 'Calmar', 'Woodward', 'Hills', 'Sabula', 'Corning', 'Fontanelle', 'Afton', 'Northwood', 'Everly', 'Waukon']

df = pd.read_csv('cities.csv')
print(list(df.columns))
exportDF = df.dropna()
exportDF = exportDF.reset_index().drop(axis=1, columns=['GEOID', 'parent-location', 'imputed', 'subbed', 'index', 'pct-multiple', 'pct-other'])
exportDF.columns = ['Year', 'City', 'Pop', 'Poverty Rate', '% Renter', 'Median Gross Rent', 'Median Household Income', 'Median Property Value', 'Rent Burden', 'White', 'African American', 'Hispanic', 'American Indian', 'Asian', 'Non-Hispanic', 'Renter Occupied Households', 'Eviction Filings', 'Evictions', 'Eviction Rate', 'Filing Rate']

cityBasics = pd.read_csv('city-basics_iowa.csv')
cityBasics['City'] = cityBasics.City.apply(lambda city: re.match('([^,]+),.*', city)[1] if re.match('([^,]+),.*', city) is not None else '')


def getLat(cityName):
    try:
        lat = float(cityBasics[cityBasics['City'] == cityName]['Lat'][0])
        return lat
    except:
        return pd.NA

def getLon(cityName):
    try:
        lon = float(cityBasics[cityBasics['City'] == cityName]['Lon'][0])
        return lon
    except:
        pd.NA

newDF = exportDF.groupby('City').agg({'Poverty Rate':'sum','% Renter':'sum','Median Gross Rent':'sum','Median Household Income':'sum','Rent Burden':'sum','Eviction Rate':'sum','Filing Rate':'sum'}).reset_index()
#  City  Poverty Rate  % Renter  Median Gross Rent  Median Household Income  Rent Burden  Eviction Rate  Filing Rate
newDF.columns = ["City", "Poverty Rate", "Renter Rate", "Median Rent", "Median Income", "Rent Burden", "Eviction Rate", "Filing Rate"]
newDF['Lat'] = newDF['City'].apply(getLat)
newDF['Lon'] = newDF['City'].apply(getLon)
newDF = newDF[newDF['City'].isin(theCities)]
#newDF = newDF.fillna(0.0)
print(newDF)