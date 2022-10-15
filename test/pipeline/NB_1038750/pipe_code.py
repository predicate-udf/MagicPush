import pickle
df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df1 = df0.groupby(["Data","País/Região"]).agg({ "Latitude":"mean","Longitude":"mean","PIB 2019":"unique","População":"unique","População Urbana":"unique","Área (Km/2)":"unique","Densidade (Km/2)":"unique","Taxa de Fertilidade":"mean","Média de Idade":"mean","Taxa de Fumantes":"mean","Taxa de Mortalidade por Doenças Pulmonares":"mean","Total de Leitos Hospitalares":"min","Temperatura Média (Janeiro - Março)":"mean","Taxa de Umidade (Janeiro - Março)":"mean","Casos Confirmados":"sum" }).reset_index().rename(columns={ "Latitude":"Latitude","Longitude":"Longitude","PIB 2019":"PIB 2019","População":"População","População Urbana":"População Urbana","Área (Km/2)":"Área (Km/2)","Densidade (Km/2)":"Densidade (Km/2)","Taxa de Fertilidade":"Taxa de Fertilidade","Média de Idade":"Média de Idade","Taxa de Fumantes":"Taxa de Fumantes","Taxa de Mortalidade por Doenças Pulmonares":"Taxa de Mortalidade por Doenças Pulmonares","Total de Leitos Hospitalares":"Total de Leitos Hospitalares","Temperatura Média (Janeiro - Março)":"Temperatura Média (Janeiro - Março)","Taxa de Umidade (Janeiro - Março)":"Taxa de Umidade (Janeiro - Março)","Casos Confirmados":"Casos Confirmados" })
df2 = df1.sort_values(by=["Data","País/Região"])
df3 = df2.drop_duplicates(subset=["País/Região"])
df4 = df3.drop(columns=["Data","Casos Confirmados"])
df5 = df2.pivot(index="País/Região", columns="Data", values="Casos Confirmados")
print(df5)
df6 = df5.sort_values(by=["index"])
df7 = df4.merge(df6, how='left', left_on = ["País/Região"], right_on = ["País/Região"])
pickle.dump(df7, open('./temp/result_original.p', 'wb'))