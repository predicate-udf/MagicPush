import sys

import pandas as pd

import re

import numpy as np
from tqdm import tqdm
import gc

from fuzzywuzzy import fuzz
import pickle
df_contribuion = pd.read_csv('contribution_processed.csv')
#pickle.dump(df_contribuion, open("contribution_processed.pickle",'wb'))

df_companies = pd.read_csv('ma-companies-on-linkedin.csv', sep = ';')
#pickle.dump(df_companies, open("ma-companies-on-linkedin.pickle",'wb'))

match_set = pickle.load(open('match_set.p','rb'))

def clean_text(text):
    text = str(text).lower()
    text = re.sub('[^A-Za-z]+', '', text)
    return text

df_contribuion['Employer_clean'] = df_contribuion['Employer'].apply(lambda x: re.sub('[^A-Za-z]+', '', str(x).lower()))
df_companies['Company_clean'] = df_companies['Company name'].apply(clean_text)

"""
companies_list = df_companies['Company name'].unique()
employer_list = df_contribuion['Employer'].unique()

companies_list_clean = np.array([clean_text(x) for x in companies_list.copy()])
employer_list_clean = np.array([clean_text(x) for x in employer_list.copy()])
# MATCHING
match = []

companies_list_clean = companies_list_clean.astype('O')

employer_list_clean = employer_list_clean.astype('O')
for employer in tqdm(employer_list_clean):

    for company in companies_list_clean:

        if fuzz.ratio(employer, company) >= 85:

            match.append(employer)
match_set = set(match)
print("match_set = {}".format(len(match_set)))
pickle.dump(match_set, open('match_set.p','wb'))
df_contribuion['Employer_clean'] = df_contribuion['Employer'].apply(clean_text)
df_companies['Company_clean'] = df_companies['Company name'].apply(clean_text)
def company_search(name):

    return name in match_set
df_contribuion['Industry_new'] = df_contribuion['Employer_clean'].apply(company_search)
df_contribuion['Industry_new'].value_counts()
match_ind = {}

for com in match_set:

    industry = df_companies[df_companies['Company_clean'] == com]['Industry'].values

    if len(industry) == 0:

        continue

    match_ind[com] = industry[0]
employer_dict = {}

for employer in tqdm(list(match_set)):

    for company in companies_list_clean:

        if fuzz.ratio(employer, company) >= 85:

            employer_dict[employer] = company
employer_dict_list = list(employer_dict.keys())
print("employer_dict = {}".format(len(employer_dict)))
pickle.dump(employer_dict, open('employer_dict.p','wb'))
"""
employer_dict = pickle.load(open('employer_dict.p', 'rb'))

def com_search(name):

    if name in employer_dict.keys():
        industry = df_companies[df_companies['Company_clean'] == employer_dict[name]]['Industry'].values[0]
        return industry

df_contribuion['Industry_new'] = df_contribuion['Employer_clean'].apply(com_search)
df_contribuion.drop(columns=['Employer_clean'], inplace=True)
print(df_contribuion.head())
