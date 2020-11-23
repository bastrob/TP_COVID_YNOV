# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 10:48:09 2020

@author: Bast
"""

import pandas as pd
import glob
import requests
import numpy as np
import geopandas as gpd


# 1) Récuperer l'ensemble des fichiers dans departements
# 2) Construction d'un dataset unique
# 3) Ajout colonne Region
# 4) Recherche coordonnées régions



# 1) Récuperer l'ensemble des fichiers dans departements
# 2) Construction d'un dataset unique

# Lire le dossier qui contient les datasets et créer un dataset unique sauvegardé dans le dossier dataset cleaned
# Le csv se mettra à jour si l'on importe de nouvelles données

final_path = r'..\..\datasets_cleaned\dataset_gouv_prepared.csv'

def concat_dataset() -> pd.DataFrame:
    path = r'..\..\datasets_raw\covid_departements_datasets' # use your path
    print(path)
    all_files = glob.glob(path + "/*.csv")
    li = []
    print(all_files)
    for filename in all_files:
        df = pd.read_csv(filename)
        df.columns = ['departement', 'date', 'hospitalises', 'reanimations', 'nvx_hospitalises', 'nvlles_reanimations', 'gueris', 'deces']
        li.append(df)

 
    dataset = pd.concat(li, ignore_index=True)

    #save_csv(dataset)
    return dataset

def save_csv(dataset: pd.DataFrame):
    dataset.to_csv(final_path, index = False, header=True)

def load_csv(path: str) -> pd.DataFrame:
    dataset = pd.read_csv(path)
    return dataset
 
#concat_dataset()



#dataset = pd.read_csv(final_path)

#print(dataset.head())

# 3) Ajout de la colonne des Régions https://www.regions-et-departements.fr/regions-francaises

# regions_france = {"Auvergne-Rhône-Alpes" : "bonjour", "Bourgogne-Franche-Comté" :, "Bretagne":, "Centre-Val de Loire":, "Corse",
#                   "Grand Est", "Hauts-de-France", "Île-de-France", "Normandie", "Nouvelle-Aquitaine":, "Occitanie", 
#                   "Pays de la Loire", "Provence-Alpes-Côte d'Azur"}

departements = ["Ain", "Aisne", "Allier", "Alpes-de-Haute-Provence",
                "Hautes-Alpes", "Alpes-Maritimes", "Ardèche", "Ardennes",
                "Ariège", "Aube", "Aude", "Aveyron", "Bouches-du-Rhône",
                "Calvados", "Cantal", "Charente", "Charente-Maritime",
                "Cher", "Corrèze", 	"Corse-du-Sud", "Haute-Corse", 
                "Côte-d'Or", "Côtes-d'Armor", "Creuse", "Dordogne",
                "Doubs", "Drôme", "Eure", "Eure-et-Loir", "Finistère",
                "Gard", "Haute-Garonne", "Gers", "Gironde", "Hérault",
                "Ille-et-Vilaine", "Indre", "Indre-et-Loire", "Isère",
                "Jura", "Landes", "Loir-et-Cher", "Loire", "Haute-Loire",
                "Loire-Atlantique", "Loiret", "Lot", "Lot-et-Garonne",
                "Lozère", "Maine-et-Loire", "Manche", "Marne", "Haute-Marne",
                "Mayenne", "Meurthe-et-Moselle", "Meuse", "Morbihan", "Moselle", 
                "Nièvre", "Nord", "Oise", "Orne", "Pas-de-Calais", "Puy-de-Dôme",
                "Pyrénées-Atlantiques", "Hautes-Pyrénées", "Pyrénées-Orientales",
                "Bas-Rhin", "Haut-Rhin", "Rhône", "Haute-Saône", "Saône-et-Loire",
                "Sarthe", "Savoie", "Haute-Savoie", "Paris", "Seine-Maritime", 
                "Seine-et-Marne", "Yvelines", "Deux-Sèvres", "Somme", "Tarn",
                "Tarn-et-Garonne", "Var", "Vaucluse", "Vendée", "Vienne", "Haute-Vienne",
                "Vosges", "Yonne", "Territoire de Belfort", "Essonne", "Hauts-de-Seine",
                "Seine-Saint-Denis", "Val-de-Marne", "Val-d'Oise"
                ]

regions_france = {"Auvergne-Rhône-Alpes" : ["Ain", "Allier", "Ardèche", "Cantal", "Drôme", "Isère", "Loire", "Haute-Loire", "Puy-de-Dôme", "Rhône", "Savoie", "Haute-Savoie"], 
                  "Bourgogne-Franche-Comté" : ["Côte-d'Or", "Doubs", "Jura", "Nièvre", "Haute-Saône", "Saône-et-Loire", "Yonne", "Territoire de Belfort"],
                  "Bretagne" : ["Côtes-d'Armor", "Finistère", "Ille-et-Vilaine", "Morbihan"],
                  "Centre-Val de Loire" : [ "Cher", "Eure-et-Loir", "Indre", "Indre-et-Loire", "Loir-et-Cher", "Loiret"],
                  "Corse" : ["Corse-du-Sud", "Haute-Corse"],
                  "Grand Est" : ["Ardennes", "Aube", "Marne", "Haute-Marne", "Meurthe-et-Moselle", "Meuse", "Moselle", "Bas-Rhin", "Haut-Rhin", "Vosges"],
                  "Hauts-de-France" : ["Aisne", "Nord", "Oise", "Pas-de-Calais", "Somme"],
                  "Île-de-France" : ["Paris", "Seine-et-Marne", "Yvelines", "Essonne", "Hauts-de-Seine", "Seine-Saint-Denis", "Val-de-Marne", "Val-d'Oise"],
                  "Normandie" : ["Calvados", "Eure", "Manche", "Orne", "Seine-Maritime"],
                  "Nouvelle-Aquitaine" : ["Charente", "Charente-Maritime", "Corrèze", "Creuse", "Dordogne", "Gironde", "Landes", "Lot-et-Garonne", "Pyrénées-Atlantiques", "Deux-Sèvres", "Vienne", "Haute-Vienne"],
                  "Occitanie" : ["Ariège", "Aude", "Aveyron", "Gard", "Haute-Garonne", "Gers", "Hérault", "Lot", "Lozère", "Hautes-Pyrénées", "Pyrénées-Orientales", "Tarn", "Tarn-et-Garonne"],
                  "Pays de la Loire" : ["Loire-Atlantique", "Maine-et-Loire", "Mayenne", "Sarthe", "Vendée"],
                  "Provence-Alpes-Côte d'Azur" : ["Alpes-de-Haute-Provence", "Hautes-Alpes", "Alpes-Maritimes", "Bouches-du-Rhône", "Var", "Vaucluse"]
                  }


def set_region(x: str) -> str:
    #print(row.departement)
    region = ""
    for key, value in regions_france.items():
        if x in value:
            region = key
    return region

def create_region(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset['region'] = dataset.departement.map(lambda x: set_region(x))
    return dataset


#Ajout des données de géolocalisations de chaques region avec l'API nominatim(OpenStreetMap)

def create_lat_long(dataset: pd.DataFrame) -> pd.DataFrame:
    regions = dataset['region'].unique()
    dataset.insert(len(dataset.columns),'lat','')
    dataset.insert(len(dataset.columns),'lon','')

    for i in regions :
        response = requests.get("https://nominatim.openstreetmap.org/search/%s?format=json&addressdetails=0&limit=1&polygon_svg=1&countrycodes=fr&extratags=1&linked_place=state" % i)
        region = pd.json_normalize(response.json())
        dataset['lat'] = np.where(dataset['region']== i, region['lat'].values, dataset['lat'])
        dataset['lon'] = np.where(dataset['region']== i, region['lon'].values, dataset['lon'])

    return dataset


#dataset = pd.read_csv(final_path)

def create_geodataframe(dataset: pd.DataFrame) -> gpd.GeoDataFrame:
    # Convert the DataFrame to a GeoDataFrame
    geodataframe = gpd.GeoDataFrame(dataset, geometry=gpd.points_from_xy(dataset.lat, dataset.lon))
    
    # Set the coordinate reference system (CRS) to EPSG 4326
    geodataframe.crs = {'init': 'epsg:4326'}
    return geodataframe


def preparation_pipeline():
    concat_dataset()
    dataset = pd.read_csv(final_path)
    dataset = create_region(dataset)
    dataset = create_lat_long(dataset)
    save_csv(dataset)
    load_csv(final_path)
    



