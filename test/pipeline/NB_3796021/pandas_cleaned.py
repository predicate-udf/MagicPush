import pandas as pd
import pickle
df0=pickle.load(open('data_0.pickle','rb'))
df0.drop(columns=["World Happiness Report 2016 (Cantril Ladder (0=worst; 10=best))"],inplace=True)
df13 = df0[df0["Year"] > 2006]
df14 = df13[df13["Entity"].isin(["Albania","Argentina","Armenia","Australia","Azerbaijan","Bangladesh","Belarus","Belgium","Belize","Bolivia","Bosnia and Herzegovina","Brazil","Bulgaria","Burkina Faso","Cambodia","Cameroon","Canada","Central African Republic","Chad","Chile","China","Colombia","Costa Rica","Croatia","Czech Republic","Denmark","Dominican Republic","Ecuador","Egypt","El Salvador","Estonia","Georgia","Germany","Ghana","Greece","Guatemala","Guyana","Honduras","Hungary","India","Indonesia","Iran","Israel","Italy","Japan","Jordan","Kazakhstan","Kenya","Kosovo","Kyrgyzstan","Laos","Latvia","Liberia","Lithuania","Macedonia","Malawi","Malaysia","Mauritania","Mexico","Moldova","Mongolia","Montenegro","Mozambique","Namibia","Nepal","Netherlands","New Zealand","Nicaragua","Niger","Nigeria","Pakistan","Palestine","Panama","Paraguay","Peru","Philippines","Poland","Romania","Russia","Saudi Arabia","Senegal","Serbia","Sierra Leone","Singapore","South Africa","South Korea","Spain","Sri Lanka","Sweden","Tajikistan","Tanzania","Thailand","Turkey","Uganda","Ukraine","United Kingdom","United States","Uruguay","Vietnam","Yemen","Zambia","Zimbabwe"])]
df18 = df14.groupby(by=["Entity"])["Cantril_Score","Year","Code"].count()

df21 = df18[df18["Code"] == 12]

