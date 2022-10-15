import pandas as pd
import pickle
import numpy as np
df1=pickle.load(open('data_1.pickle','rb'))
df3 = df1.drop(columns=["imdb_id","homepage","tagline","keywords","overview","production_companies","budget_adj","revenue_adj"])
df3.drop_duplicates(subset=["id","popularity","budget","revenue","original_title","cast","director","runtime","genres","release_date","vote_count","vote_average","release_year"], inplace=True)
df19 = df3.drop(columns=["id","runtime","vote_count","release_date"])
df20 = df19.sort_values(["popularity"])
df20['new_col'] = df20['revenue']-df20['budget']
