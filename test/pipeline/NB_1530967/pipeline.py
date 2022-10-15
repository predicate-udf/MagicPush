import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")

import z3
import dis
from interface import *
from util import *
import random
# from constraint import *
from predicate import *

theCities = ['Batavia', 'Farmington', 'Denison', 'Atkins', 'Elgin', 'Williamsburg', 'Le_Claire', 'Radcliffe', 'Hopkinton', 'Webster_City', 'Altoona', 'University_Park', 'Battle_Creek', 'Ainsworth', 'Villisca', 'Colo', 'Bayard', 'Donnellson', 'Sheffield', 'Albert_City', 'Hartford', 'Mount_Ayr', 'Epworth', 'Humboldt', 'Le_Mars', 'Albion', 'Dumont', 'St_Charles', 'Ventura', 'Bancroft', 'Olin', 'Fruitland', 'St_Ansgar', 'Sibley', 'Bedford', 'Grand_Mound', 'Hartley', 'George', 'Missouri_Valley', 'Graettinger', 'What_Cheer', 'Albia', 'Hospers', 'Center_Point', 'Clarence', 'Estherville', 'Pomeroy', 'Monroe', 'Gilbertville', 'Gowrie', 'Kanawha', 'Quasqueton', 'Lamoni', 'Murray', 'Sac_City', 'Moulton', 'Mapleton', 'Montezuma', 'Winfield', 'Correctionville', 'Pocahontas', 'Nashua', 'Eagle_Grove', 'Columbus_Junction', 'Audubon', 'Traer', 'Cherokee', 'Hamburg', 'Waverly', 'Mediapolis', 'Pleasantville', 'Glidden', 'Bloomfield', 'Anita', 'Nora_Springs', 'Elma', 'Agency', 'Elk_Horn', 'Jefferson', 'Pacific_Junction', 'Lake_Park', 'Wellsburg', 'Chariton', 'Clarinda', 'Allerton', 'Madrid', 'Garnavillo', 'Buffalo_Center', 'Calmar', 'Woodward', 'Hills', 'Sabula', 'Corning', 'Fontanelle', 'Afton', 'Northwood', 'Everly', 'Waukon']

df = InitTable('cities.csv')
exportDF1 = DropNA(df, ['GEOID', 'year', 'name', 'parent-location', 'population', 'poverty-rate', 'pct-renter-occupied', 'median-gross-rent', 'median-household-income', 'median-property-value', 'rent-burden', 'pct-white', 'pct-af-am', 'pct-hispanic', 'pct-am-ind', 'pct-asian', 'pct-nh-pi', 'pct-multiple', 'pct-other', 'renter-occupied-households', 'eviction-filings', 'evictions', 'eviction-rate', 'eviction-filing-rate', 'imputed', 'subbed'])
exportDF2 = DropColumns(exportDF1, ['GEOID', 'parent-location', 'imputed', 'subbed', 'index', 'pct-multiple', 'pct-other'])
exportDF3 = Rename(exportDF2, {'year': 'Year', 'name': 'City', 'population': 'Pop', 'poverty-rate': 'Poverty Rate', 'pct-renter-occupied': '% Renter', 'median-gross-rent': 'Median Gross Rent', 'median-household-income': 'Median Household Income', 'median-property-value': 'Median Property Value', 'rent-burden': 'Rent Burden', 'pct-white': 'White', 'pct-af-am': 'African American', 'pct-hispanic': 'Hispanic', 'pct-am-ind': 'American Indian', 'pct-asian': 'Asian', 'pct-nh-pi': 'Non-Hispanic', 'renter-occupied-households': 'Renter Occupied Households', 'eviction-filings': 'Eviction Filings', 'evictions': 'Evictions', 'eviction-rate': 'Eviction Rate', 'eviction-filing-rate': 'Filing Rate'})

cityBasics = InitTable('city-basics_iowa.csv')
cityBasics1 = SetItem(cityBasics, 'City', "lambda city: re.match('([^,]+),.*', city['City'])[1] if re.match('([^,]+),.*', city['City']) is not None else ''", 'str')

newDF = GroupBy(exportDF3, ['City'], {'Poverty Rate':'sum','% Renter':'sum','Median Gross Rent':'sum','Median Household Income':'sum','Rent Burden':'sum','Eviction Rate':'sum','Filing Rate':'sum'}, \
    {"Poverty Rate":"Poverty Rate", "% Renter": "Renter Rate", 'Median Gross Rent':"Median Rent", 'Median Household Income':"Median Income", 'Rent Burden':"Rent Burden", 'Eviction Rate':"Eviction Rate", 'Filing Rate':"Filing Rate"})

sub_op0 = SubpipeInput(cityBasics1, 'table')
sub_op_row = SubpipeInput(newDF, 'row')
filter_row = ScalarComputation({'row':sub_op_row},"lambda row: row['City']")
sub_op1 = Filter(sub_op0, BinOp(Field('City'), '==', filter_row))
sub_op2 = AllAggregate(sub_op1, Value(0), "lambda x,y: y['Lat']")
cond_helper = AllAggregate(sub_op1, Value(0), "lambda x,y: x+1")
cond1 = ScalarComputation({'count':cond_helper}, 'lambda count: count>0')
cond2 = ScalarComputation({'count':cond_helper}, 'lambda count: count==0')
rs2 = ScalarComputation({'row':sub_op_row}, 'lambda row: 0')
newDF1 = CrosstableUDF(newDF, "Lat", SubPipeline([\
    PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, sub_op2], cond1), \
    PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, rs2], cond2)]))

sub_op0 = SubpipeInput(cityBasics1, 'table')
sub_op_row = SubpipeInput(newDF1, 'row')
filter_row = ScalarComputation({'row':sub_op_row},"lambda row: row['City']")
sub_op1 = Filter(sub_op0, BinOp(Field('City'), '==', filter_row))
sub_op2 = AllAggregate(sub_op1, Value(0), "lambda x,y: y['Lon']")
cond_helper = AllAggregate(sub_op1, Value(0), "lambda x,y: x+1")
cond1 = ScalarComputation({'count':cond_helper}, 'lambda count: count>0')
cond2 = ScalarComputation({'count':cond_helper}, 'lambda count: count==0')
rs2 = ScalarComputation({'row':sub_op_row}, 'lambda row: 0')
newDF2 = CrosstableUDF(newDF1, "Lon", SubPipeline([\
    PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, sub_op2], cond1), \
    PipelinePath([sub_op0, sub_op_row, filter_row, sub_op1, cond_helper, rs2], cond2)]))

newDF3 = Filter(newDF2, BinOp(Field('City'), 'in', Constant(['Batavia', 'Farmington', 'Denison', 'Atkins', 'Elgin', 'Williamsburg', 'Le_Claire', 'Radcliffe', 'Hopkinton', 'Webster_City', 'Altoona', 'University_Park', 'Battle_Creek', 'Ainsworth', 'Villisca', 'Colo', 'Bayard', 'Donnellson', 'Sheffield', 'Albert_City', 'Hartford', 'Mount_Ayr', 'Epworth', 'Humboldt', 'Le_Mars', 'Albion', 'Dumont', 'St_Charles', 'Ventura', 'Bancroft', 'Olin', 'Fruitland', 'St_Ansgar', 'Sibley', 'Bedford', 'Grand_Mound', 'Hartley', 'George', 'Missouri_Valley', 'Graettinger', 'What_Cheer', 'Albia', 'Hospers', 'Center_Point', 'Clarence', 'Estherville', 'Pomeroy', 'Monroe', 'Gilbertville', 'Gowrie', 'Kanawha', 'Quasqueton', 'Lamoni', 'Murray', 'Sac_City', 'Moulton', 'Mapleton', 'Montezuma', 'Winfield', 'Correctionville', 'Pocahontas', 'Nashua', 'Eagle_Grove', 'Columbus_Junction', 'Audubon', 'Traer', 'Cherokee', 'Hamburg', 'Waverly', 'Mediapolis', 'Pleasantville', 'Glidden', 'Bloomfield', 'Anita', 'Nora_Springs', 'Elma', 'Agency', 'Elk_Horn', 'Jefferson', 'Pacific_Junction', 'Lake_Park', 'Wellsburg', 'Chariton', 'Clarinda', 'Allerton', 'Madrid', 'Garnavillo', 'Buffalo_Center', 'Calmar', 'Woodward', 'Hills', 'Sabula', 'Corning', 'Fontanelle', 'Afton', 'Northwood', 'Everly', 'Waukon'])) )
