import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df1=pickle.load(open('data_1.pickle','rb'))
df5 = df0.merge(df1, how='left', left_on=["id"],right_on=["id"])
df9 = df5['categories'].str.split(";",expand=True)
#df10 = pd.concat([df1,df9], axis=1)
df10 = df1.merge(df9, left_index=True, right_index=True)
df10.drop(columns=["categories"],inplace=True)
df10.rename(columns={ "id":"id",0:"related",1:"request",2:"offer",3:"aid_related",4:"medical_help",5:"medical_products",6:"search_and_rescue",7:"security",8:"military",9:"child_alone",10:"water",11:"food",12:"shelter",13:"clothing",14:"money",15:"missing_people",16:"refugees",17:"death",18:"other_aid",19:"infrastructure_related",20:"transport",21:"buildings",22:"electricity",23:"tools",24:"hospitals",25:"shops",26:"aid_centers",27:"other_infrastructure",28:"weather_related",29:"floods",30:"storm",31:"fire",32:"earthquake",33:"cold",34:"other_weather",35:"direct_report" }, inplace=True)
df20 = df5.drop(columns=["categories"])
