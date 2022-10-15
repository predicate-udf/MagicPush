# 1075768.py
import sys
sys.path.append("../../")
sys.path.append("../")
import z3
import dis
from test_helper import *
from pandas_op import *
from util import *
import random
from constraint import *
from infer_schema import *
from predicate import *
from generate_input_filters import *
import numpy as np
import random
import pickle
"""
demographics = pd.read_csv()
demographics = demographics[demographics['Question Key'] != 'BEGIN QUESTIONNAIRE']
demographics = demographics[demographics['Question Key'] != 'END QUESTIONNAIRE']
demographics['id'] = demographics['Participant Private ID']
demographics['question'] = demographics['Question Key']
demographics['response'] = demographics['Response']
demographics = demographics[['id', 'question', 'response']]
d = pd.pivot_table(demographics, index = 'id', columns = 'question', values = 'response', aggfunc='first')
d = d.reset_index()
d.columns= ['id','age', 'confirmation_1', 'confirmation_2', 'gender', 'gender_numeric','handedness', 'handedness_numeric', 'degree', 'degree_numeric']
d.to_csv()
"""
raw_data = pd.read_csv()
schema = get_sc


