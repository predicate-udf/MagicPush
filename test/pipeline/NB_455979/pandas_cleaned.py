import pandas as pd
import pickle

titanic_data = pd.read_csv('Titanic_Dataset.csv')

def imput_age(cols):
    Age = cols[0]
    Pclass = cols[1]
    if pd.isnull(Age):
        return titanic_data[titanic_data["Pclass"] == Pclass]["Age"].mean() 
    else:
        return Age

titanic_data["Age"] = titanic_data[["Age", "Pclass"]].apply(imput_age,axis=1)
titanic_data['Embarked'] = titanic_data['Embarked'].fillna('S')
sex = pd.get_dummies(titanic_data['Sex'])
embark = pd.get_dummies(titanic_data['Embarked'])
pclass = pd.get_dummies(titanic_data["Pclass"])
print(sex.columns)
titanic_data.drop(['PassengerId','Sex','Embarked','Name','Ticket','Pclass'],axis=1,inplace=True)
titanic_data = pd.concat([titanic_data,sex,embark,pclass],axis=1)
print(titanic_data.columns)