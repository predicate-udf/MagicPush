import pandas as pd
import pickle

df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df1 = df0.drop(columns=["World Happiness Report 2016 (Cantril Ladder (0=worst; 10=best))"])
df2 = df1[df1.apply(lambda row: row['Year'] > 2006, axis=1)]
df3 = df2[df2.apply(lambda row: (row['Entity'] == 'Albania') or (row['Entity'] == 'Argentina') or (row['Entity'] == 'Armenia') or (row['Entity'] == 'Australia') or (row['Entity'] == 'Azerbaijan') or (row['Entity'] == 'Bangladesh') or (row['Entity'] == 'Belarus') or (row['Entity'] == 'Belgium') or (row['Entity'] == 'Belize') or (row['Entity'] == 'Bolivia') or (row['Entity'] == 'Bosnia and Herzegovina') or (row['Entity'] == 'Brazil') or (row['Entity'] == 'Bulgaria') or (row['Entity'] == 'Burkina Faso') or (row['Entity'] == 'Cambodia') or (row['Entity'] == 'Cameroon') or (row['Entity'] == 'Canada') or (row['Entity'] == 'Central African Republic') or (row['Entity'] == 'Chad') or (row['Entity'] == 'Chile') or (row['Entity'] == 'China') or (row['Entity'] == 'Colombia') or (row['Entity'] == 'Costa Rica') or (row['Entity'] == 'Croatia') or (row['Entity'] == 'Czech Republic') or (row['Entity'] == 'Denmark') or (row['Entity'] == 'Dominican Republic') or (row['Entity'] == 'Ecuador') or (row['Entity'] == 'Egypt') or (row['Entity'] == 'El Salvador') or (row['Entity'] == 'Estonia') or (row['Entity'] == 'Georgia') or (row['Entity'] == 'Germany') or (row['Entity'] == 'Ghana') or (row['Entity'] == 'Greece') or (row['Entity'] == 'Guatemala') or (row['Entity'] == 'Guyana') or (row['Entity'] == 'Honduras') or (row['Entity'] == 'Hungary') or (row['Entity'] == 'India') or (row['Entity'] == 'Indonesia') or (row['Entity'] == 'Iran') or (row['Entity'] == 'Israel') or (row['Entity'] == 'Italy') or (row['Entity'] == 'Japan') or (row['Entity'] == 'Jordan') or (row['Entity'] == 'Kazakhstan') or (row['Entity'] == 'Kenya') or (row['Entity'] == 'Kosovo') or (row['Entity'] == 'Kyrgyzstan') or (row['Entity'] == 'Laos') or (row['Entity'] == 'Latvia') or (row['Entity'] == 'Liberia') or (row['Entity'] == 'Lithuania') or (row['Entity'] == 'Macedonia') or (row['Entity'] == 'Malawi') or (row['Entity'] == 'Malaysia') or (row['Entity'] == 'Mauritania') or (row['Entity'] == 'Mexico') or (row['Entity'] == 'Moldova') or (row['Entity'] == 'Mongolia') or (row['Entity'] == 'Montenegro') or (row['Entity'] == 'Mozambique') or (row['Entity'] == 'Namibia') or (row['Entity'] == 'Nepal') or (row['Entity'] == 'Netherlands') or (row['Entity'] == 'New Zealand') or (row['Entity'] == 'Nicaragua') or (row['Entity'] == 'Niger') or (row['Entity'] == 'Nigeria') or (row['Entity'] == 'Pakistan') or (row['Entity'] == 'Palestine') or (row['Entity'] == 'Panama') or (row['Entity'] == 'Paraguay') or (row['Entity'] == 'Peru') or (row['Entity'] == 'Philippines') or (row['Entity'] == 'Poland') or (row['Entity'] == 'Romania') or (row['Entity'] == 'Russia') or (row['Entity'] == 'Saudi Arabia') or (row['Entity'] == 'Senegal') or (row['Entity'] == 'Serbia') or (row['Entity'] == 'Sierra Leone') or (row['Entity'] == 'Singapore') or (row['Entity'] == 'South Africa') or (row['Entity'] == 'South Korea') or (row['Entity'] == 'Spain') or (row['Entity'] == 'Sri Lanka') or (row['Entity'] == 'Sweden') or (row['Entity'] == 'Tajikistan') or (row['Entity'] == 'Tanzania') or (row['Entity'] == 'Thailand') or (row['Entity'] == 'Turkey') or (row['Entity'] == 'Uganda') or (row['Entity'] == 'Ukraine') or (row['Entity'] == 'United Kingdom') or (row['Entity'] == 'United States') or (row['Entity'] == 'Uruguay') or (row['Entity'] == 'Vietnam') or (row['Entity'] == 'Yemen') or (row['Entity'] == 'Zambia') or (row['Entity'] == 'Zimbabwe'), axis=1)]


df4 = df3.groupby(["Entity"]).agg({ "Cantril_Score":"count","Year":"count","Entity":"count","Code":"count" }).rename(columns={ "Cantril_Score":"Cantril_Score","Year":"Year","Entity":"Entity","Code":"Code" })
df5 = df4[df4.apply(lambda row: row['Code'] == 12, axis=1)]
pickle.dump(df5, open('./temp/result_original.p', 'wb'))