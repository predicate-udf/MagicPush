import sys
# from ppl_interface import *
sys.path.append("../../../")
sys.path.append("../../")
import z3
import dis
from interface import *
from util import *
import random
from constraint import *
from predicate import *
from generate_input_filters import *
from compare_pushdown_result import get_output_filter, check_pushdown_result
import numpy as np
import os

op0 = InitTable("data_0.pickle")
op1 = Rename(op0, { "Life expectancy at birth (years)  Both sexes":"LE_both" })
op2 = Rename(op1, { "Life expectancy at birth (years)  Male":"LE_male" })
op3 = Rename(op2, { "Life expectancy at birth (years)  Female":"LE_female" })
op4 = Rename(op3, { "GDP per Capita":"GDP" })
op5 = Rename(op4, { "Surface area (sq. km)":"Health_expenditure" })
op6 = Rename(op5, { "Population, total":"RnD" })
#op7 = Filter(op6, And(BinOp(Field("Country"),'==',Constant("South Sudan")),BinOp(Field("Year"),'<=',Constant(2010))))
op8 = DropColumns(op6, ["index"])
op9 = SetItem(op8, "Income Level", 'lambda xxx__: "Unknown" if xxx__["Income Level"] == np.nan else xxx__["Income Level"]' )
op13 = DropNA(op9, ["Income Level"])
op14 = SetItem(op13, "Income Level", 'lambda xxx__: np.nan if xxx__["Income Level"] == "Unknown" else xxx__["Income Level"]' )
op19 = Copy(op14)
op20 = Filter(op19, BinOp(Field("Year"),'==',Constant(2015)))
op21 = DropColumns(op20, ['Country', 'Year', 'LE_male', 'LE_female'])
op22 = SetItem(op21, "Income Level", 'lambda xxx__: {\'H\':0, \'L\':1, \'LM\':2, \'UM\':3}[xxx__["Income Level"]] if xxx__["Income Level"] in [\'H\', \'L\', \'LM\', \'UM\'] else xxx__["Income Level"]')
op24 = ChangeType(op22, 'int', "Income Level", "Income Level")
op21_0 = FillNA(op24, "LE_both", 0)
op21_1 = FillNA(op21_0, "Income Level", 0)
op21_2 = FillNA(op21_1, "GDP", 0)
op21_3 = FillNA(op21_2, "Health_expenditure", 0)
op21_4 = FillNA(op21_3, "RnD", 0)
op21_5 = FillNA(op21_4, "Population density (people per sq. km of land area)", 0)
op21_6 = FillNA(op21_5, "PM2.5 air pollution, mean annual exposure (micrograms per cubic meter)", 0)
op21_7 = FillNA(op21_6, "Mortality caused by road traffic injury (per 100,000 people)", 0)
op21_8 = FillNA(op21_7, "Last Updated: 11/14/2018", 0)
op21_9 = FillNA(op21_8, "Intentional homicides (per 100,000 people)", 0)
op21_10 = FillNA(op21_9, "Individuals using the Internet (% of population)", 0)
op21_11 = FillNA(op21_10, "Incidence of tuberculosis (per 100,000 people)", 0)
op21_12 = FillNA(op21_11, "GNI, Atlas method (current US$)", 0)
op21_13 = FillNA(op21_12, "GDP per capita growth (annual %)", 0)
op21_14 = FillNA(op21_13, "GDP growth (annual %)", 0)
op21_15 = FillNA(op21_14, "Data from database: World Development Indicators", 0)
op21_16 = FillNA(op21_15, "CO2 emissions (metric tons per capita)", 0)
op21_17 = FillNA(op21_16, "Access to electricity (% of population)", 0)
op21_18 = FillNA(op21_17, "Current health expenditure (CHE) per capita in US$", 0)
op25 = FillNA(op21_18, "Research and development expenditure (% of GDP)", 0)


ops = [op0, op1, op2, op3, op4, op5, op6, op7, op8, op9, op13, op14, op19, op20, op21, op22, op24, op21_0, op21_1, op21_2, op21_3, op21_4, op21_5, op21_6, op21_7, op21_8, op21_9, op21_10, op21_11, op21_12, op21_13, op21_14, op21_15, op21_16, op21_17, op21_18, op25]
print(len(ops))
output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = get_output_filter(ops, './temp')

print(output_filter)
#print(output_filter)
#print(output_filter)
#exit(0)
for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = {None:output_filter}
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    inference_i = op_i.get_inference_instance(output_filter_i)
    inference_i.input_filters = generate_input_filters(op_i, inference_i, AllOr(*list(output_filter_i.values())))
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    #print(inference_i.output_filter)

# check_pushdown_result(ops, 'temp/')