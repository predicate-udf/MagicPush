import pandas as pd
import pickle
import numpy as np
df85=pickle.load(open('data_85.pickle','rb'))
df86 = df85.sort_values(["2016"])
print(df86.columns)
df90 = df86.iloc[:20].reset_index(drop=True)
print(df90.columns)
df92 = df85.sort_values(["1960"])
df96 = df92.iloc[:20].reset_index(drop=True)
df101 = df96.append(df90)
df102 = df101.drop_duplicates(subset=["Country Name","Country Code","1960","1961","1962","1963","1964","1965","1966","1967","1968","1969","1970","1971","1972","1973","1974","1975","1976","1977","1978","1979","1980","1981","1982","1983","1984","1985","1986","1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016"])
df103 = df102.reset_index(drop=True)
