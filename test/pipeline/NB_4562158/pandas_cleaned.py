import pandas as pd
import pickle
import numpy as np
df1=pickle.load(open('data_1.pickle','rb'))
df2 = df1.drop(columns=["Unnamed: 0","Organization Name URL"])
df3 = df2.drop_duplicates(subset=["Organization Name"])
df6 = df3[df3["Group Gender"].notnull()]
df16 = df6[df6["Category Groups"].notnull()]
df19 = df16[df16["Headquarters Regions"].notnull()]
df19['Categories'] = df19.apply(lambda xxx__: 'Finance' if 'Finance' in xxx__['Category Groups'] else \
('Biotechnology' if 'Biotechnology' in xxx__['Category Groups'] else \
('Health Care' if 'Health Care' in xxx__['Category Groups'] else \
('E-Commerce' if 'E-Commerce' in xxx__['Category Groups'] else \
('Software' if 'Software' in xxx__['Category Groups'] else \
('Internet' if 'Internet' in xxx__['Category Groups'] else \
('Information Technology' if 'Information Technology' in xxx__['Category Groups'] else \
('Education' if 'Education' in xxx__['Category Groups'] else \
('Security' if 'Security' in xxx__['Category Groups'] else \
('Education' if 'Education' in xxx__['Category Groups'] else \
('Real Estate' if 'Real Estate' in xxx__['Category Groups'] else \
('Tourism' if 'Tourism' in xxx__['Category Groups'] else \
('Artificial Intelligence' if 'Artificial Intelligence' in xxx__['Category Groups'] else \
('Food' if 'Food' in xxx__['Category Groups'] else \
('Advertising' if 'Advertising' in xxx__['Category Groups'] else \
('Fashion' if 'Fashion' in xxx__['Category Groups'] else \
('Data' if 'Data' in xxx__['Category Groups'] else \
('Robotics' if 'Robotics' in xxx__['Category Groups'] else \
('Gaming' if 'Gaming' in xxx__['Category Groups'] else \
('Sports' if 'Sports' in xxx__['Category Groups'] else \
('Entertainment' if 'Entertainment' in xxx__['Category Groups'] else \
('Insurance' if 'Insurance' in xxx__['Category Groups'] else \
('Unknown')))))))))))))))))))))), axis=1)
df172 = df19[df19["Categories"] == "Finance"]
df168=pickle.load(open('data_168.pickle','rb'))
df173 = df168.append(df172)
df65=pickle.load(open('data_65.pickle','rb'))
df176 = df173.append(df65)
df179 = df19[df19["Categories"] == "Health Care"]
df180 = df176.append(df179)
df183 = df19[df19["Categories"] == "Internet"]
df184 = df180.append(df183)
df187 = df19[df19["Categories"] == "Biotechnology"]
df188 = df184.append(df187)
df191 = df19[df19["Categories"] == "Artificial Intelligence"]
df192 = df188.append(df191)
df194 = df19[df19["Categories"] == "Information Technology"]
df195 = df192.append(df194)
df196 = df19[df19["Categories"] == "Education"]
df197 = df195.append(df196)
df200 = df19[df19["Categories"] == "Advertising"]
df201 = df197.append(df200)
df204 = df19[df19["Categories"] == "Data"]
df205 = df201.append(df204)
df207 = df19[df19["Categories"] == "Food"]
df208 = df205.append(df207)
df211 = df19[df19["Categories"] == "Real Estate"]
df212 = df208.append(df211)
df215 = df19[df19["Categories"] == "Security"]
df216 = df212.append(df215)
df217=pickle.load(open('data_217.pickle','rb'))
df219 = df216.append(df217)
df222 = df19[df19["Categories"] == "Gaming"]
df223 = df219.append(df222)
df226 = df19[df19["Categories"] == "Robotics"]
df227 = df223.append(df226)
df230 = df19[df19["Categories"] == "Fashion"]
df231 = df227.append(df230)
df234 = df19[df19["Categories"] == "Sports"]
df235 = df231.append(df234)
df238 = df19[df19["Categories"] == "Tourism"]
df239 = df235.append(df238)
df242 = df19[df19["Categories"] == "Insurance"]
df243 = df239.append(df242)
df246 = df243[df243["Headquarters Regions"] == "European Union (EU)"]
df248 = df243[df243["Headquarters Regions"] == "San Francisco Bay Area, West Coast, Western US"]
df249 = df246.append(df248)
df181=pickle.load(open('data_181.pickle','rb'))
df252 = df249.append(df181)
df255 = df243[df243["Headquarters Regions"] == "Greater New York Area, East Coast, Northeastern US"]
df256 = df252.append(df255)
df259 = df243[df243["Headquarters Regions"] == "Greater Los Angeles Area, West Coast, Western US"]
df174 = df256.append(df259)
