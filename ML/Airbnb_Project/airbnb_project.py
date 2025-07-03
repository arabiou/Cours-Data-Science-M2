# Étape 1 : Import des librairies
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Pour l'affichage clair
pd.set_option('display.max_columns', 100)

# Étape 2 : Chargement des données
df = pd.read_csv('airbnb_train.csv')

# Étape 3 : Aperçu des données
print("Dimensions :", df.shape)
display(df.head())

# Étape 4 : Types de variables
display(df.dtypes)

# Étape 5 : Valeurs manquantes
missing = df.isnull().sum()
missing = missing[missing > 0].sort_values(ascending=False)
print("\nColonnes avec valeurs manquantes :")
display(missing)
