import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df0.rename(columns={ "SNo":"sno","ObservationDate":"observationdate","Province/State":"province/state","Country/Region":"country/region","Last Update":"last update","Confirmed":"confirmed","Deaths":"deaths","Recovered":"recovered" }, inplace=True)
df0.rename(columns={ "sno":"sno","observationdate":"observationdate","province/state":"province/state","country/region":"country/region","last update":"last_update","confirmed":"confirmed","deaths":"deaths","recovered":"recovered" }, inplace=True)
df0["country/region"] = df0.apply(lambda xxx__: xxx__["country/region"].lstrip(), axis=1)
df45 = df0[(df0["country/region"] != "Others") & (df0["country/region"] != "Diamond Princess")]
df47 = df45[df45["country/region"] != "MS Zaandam"]
df49 = df47[df47["country/region"] != "Kosovo"]
df51 = df49[df49["country/region"] != "Holy See"]
df53 = df51[df51["country/region"] != "Vatican City"]
df55 = df53[df53["country/region"] != "Timor-Leste"]
df57 = df55[df55["country/region"] != "East Timor"]
df59 = df57[df57["country/region"] != "Channel Islands"]
df61 = df59[df59["country/region"] != "Western Sahara"]
df93 = df61.groupby(by=["observationdate","country/region"]).agg({'confirmed':'sum'}).reset_index()
df96 = df93[df93["confirmed"] >= 100]
df97 = df96.sort_values(["observationdate"])
df98 = df97.drop_duplicates(subset=["country/region"])
df74 = df98.reset_index()
