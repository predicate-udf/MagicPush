import pandas as pd
import pickle

titanic = pd.read_csv('titanic.csv')
def c_deck_survival(df_in):    
    
    c_passengers = df_in['cabin'].str.startswith('C').fillna(False)
    print(df_in.loc[c_passengers, 'survived'])
    return df_in.loc[c_passengers, 'survived'].mean()

out = titanic.groupby('sex').apply(c_deck_survival).reset_index()

print(out)