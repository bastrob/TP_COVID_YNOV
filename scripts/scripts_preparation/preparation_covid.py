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

departements_population = { "Ain": 631877, "Aisne": 538659, "Allier": 341613, "Alpes-de-Haute-Provence": 161799,
                           "Hautes-Alpes": 140916, "Alpes-Maritimes": 1080440, "Ardèche": 324209, "Ardennes": 277752,
                           "Ariège": 152499, "Aube": 309056, "Aude": 366957, "Aveyron": 279169, "Bouches-du-Rhône": 2016622,
                           "Calvados": 693579, "Cantal": 146219, "Charente": 353613, "Charente-Maritime": 639938,
                           "Cher": 308992, "Corrèze": 241871, "Corse-du-Sud": 152730, "Haute-Corse": 174553, 
                           "Côte-d'Or": 533147, "Côtes-d'Armor": 598357, "Creuse": 120365, "Dordogne": 415417,
                           "Doubs": 536959 , "Drôme": 504637, "Eure": 601948, "Eure-et-Loir": 434035, "Finistère": 907796,
                           "Gard": 738189, "Haute-Garonne": 1335103, "Gers": 190932, "Gironde": 1548478, "Hérault": 1120190,
                           "Ille-et-Vilaine": 1042884, "Indre": 224200, "Indre-et-Loire": 604966, "Isère": 1251060,
                           "Jura": 260587, "Landes": 403234, "Loir-et-Cher": 333050, "Loire": 759411, "Haute-Loire": 227034,
                           "Loire-Atlantique": 1365227, "Loiret": 673349, "Lot": 173400, "Lot-et-Garonne": 333417,
                           "Lozère": 76309, "Maine-et-Loire": 810186, "Manche": 499287, "Marne": 572293, "Haute-Marne": 179154,
                           "Mayenne": 307940, "Meurthe-et-Moselle": 734403, "Meuse": 190626, "Morbihan": 744813, "Moselle": 1044486, 
                           "Nièvre": 211747, "Nord": 2605238, "Oise": 821552, "Orne": 286618, "Pas-de-Calais": 1472648, "Puy-de-Dôme": 647501,
                           "Pyrénées-Atlantiques": 670032, "Hautes-Pyrénées": 228582, "Pyrénées-Orientales": 471038,
                           "Bas-Rhin": 1116658, "Haut-Rhin": 762607, "Rhône": 1821995, "Haute-Saône": 237706, "Saône-et-Loire": 555408,
                           "Sarthe": 568445, "Savoie": 428204, "Haute-Savoie": 793938, "Paris": 2206488, "Seine-Maritime": 1257699, 
                           "Seine-et-Marne": 1390121, "Yvelines": 1427291, "Deux-Sèvres": 374435, "Somme": 571879, "Tarn": 386543,
                           "Tarn-et-Garonne": 255274, "Var": 1048652, "Vaucluse": 557548, "Vendée": 666714, "Vienne": 434887, "Haute-Vienne": 375795,
                           "Vosges": 372016, "Yonne": 340903, "Territoire de Belfort": 144483, "Essonne": 1276233, "Hauts-de-Seine": 1601569,
                           "Seine-Saint-Denis": 1592663, "Val-de-Marne": 1372389, "Val-d'Oise": 1215390                    
                           }

codes_departements = {"01" : "Ain",
                      "02" : "Aisne",
                      "03" : "Allier",
                      "04" : "Alpes-de-Haute-Provence",
                      "05" : "Hautes-Alpes",
                      "06" : "Alpes-Maritimes",
                      "07" : "Ardèche",
                      "08" : "Ardennes",
                      "09" : "Ariège",
                      "10" : "Aube",
                      "11" : "Aude",
                      "12" : "Aveyron",
                      "13" : "Bouches-du-Rhône",
                      "14" : "Calvados",
                      "15" : "Cantal",
                      "16" : "Charente",
                      "17" : "Charente-Maritime",
                      "18" : "Cher",
                      "19" : "Corrèze",
                      "2A" : "Corse-du-Sud",
                      "2B" : "Haute-Corse",
                      "21" : "Côte-d'Or",
                      "22" : "Côtes d'Armor",
                      "23" : "Creuse",
                      "24" : "Dordogne",
                      "25" : "Doubs",
                      "26" : "Drôme",
                      "27" : "Eure",
                      "28" : "Eure-et-Loir",
                      "29" : "Finistère",
                      "30" : "Gard",
                      "31" : "Haute-Garonne",
                      "32" : "Gers",
                      "33" : "Gironde",
                      "34" : "Hérault",
                      "35" : "Ille-et-Vilaine",
                      "36" : "Indre",
                      "37" : "Indre-et-Loire",
                      "38" : "Isère",
                      "39" : "Jura",
                      "40" : "Landes",
                      "41" : "Loir-et-Cher",
                      "42" : "Loire",
                      "43" : "Haute-Loire",
                      "44" : "Loire-Atlantique",
                      "45" : "Loiret",
                      "46" : "Lot",
                      "47" : "Lot-et-Garonne",
                      "48" : "Lozère",
                      "49" : "Maine-et-Loire",
                      "50" : "Manche",
                      "51" : "Marne",
                      "52" : "Haute-Marne",
                      "53" : "Mayenne",
                      "54" : "Meurthe-et-Moselle",
                      "55" : "Meuse",
                      "56" : "Morbihan",
                      "57" : "Moselle",
                      "58" : "Nièvre",
                      "59" : "Nord",
                      "60" : "Oise",
                      "61" : "Orne",
                      "62" : "Pas-de-Calais",
                      "63" : "Puy-de-Dôme",
                      "64" : "Pyrénées-Atlantiques",
                      "65" : "Hautes-Pyrénées",
                      "66" : "Pyrénées-Orientales",
                      "67" : "Bas-Rhin",
                      "68" : "Haut-Rhin",
                      "69" : "Rhône",
                      "70" : "Haute-Saône",
                      "71" : "Saône-et-Loire",
                      "72" : "Sarthe",
                      "73" : "Savoie",
                      "74" : "Haute-Savoie",
                      "75" : "Paris",
                      "76" : "Seine-Maritime",
                      "77" : "Seine-et-Marne",
                      "78" : "Yvelines",
                      "79" : "Deux-Sèvres",
                      "80" : "Somme",
                      "81" : "Tarn",
                      "82" : "Tarn-et-Garonne",
                      "83" : "Var",
                      "84" : "Vaucluse",
                      "85" : "Vendée",
                      "86" : "Vienne",
                      "87" : "Haute-Vienne",
                      "88" : "Vosges",
                      "89" : "Yonne",
                      "90" : "Territoire-de-Belfort",
                      "91" : "Essonne",
                      "92" : "Hauts-de-Seine",
                      "93" : "Seine-Saint-Denis",
                      "94" : "Val-de-Marne",
                      "95" : "Val-D'Oise"}

def set_region(x: str) -> str:
    #print(row.departement)
    region = ""
    for key, value in regions_france.items():
        if x in value:
            region = key
    return region

def set_codes(x: str) -> str:
    #print(row.departement)
    code = ""
    for key, value in codes_departements.items():
        if x in value:
            code = key
    return code

def set_population(x: str) -> int:
    population = departements_population.get(x, 0)
    return population

def create_region(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset['region'] = dataset.departement.map(lambda x: set_region(x))
    return dataset
    
def create_code(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset['code'] = dataset.departement.map(lambda x: set_codes(x))
    return dataset

def create_population(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset['population'] = dataset.departement.map(lambda x: set_population(x))
    return dataset
  
#Ajout des données de géolocalisations de chaques regions avec l'API nominatim(OpenStreetMap)

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

def create_lat_long_departement(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset.insert(len(dataset.columns),'lat_d','')
    dataset.insert(len(dataset.columns),'lon_d','')
    codes = dataset['code'].unique()
    codes = filter(None, codes)

    for j in codes :
        response = requests.get("https://nominatim.openstreetmap.org/search/%s?format=json&addressdetails=0&limit=1&polygon_svg=1&countrycodes=fr&extratags=1&border_type=departement" % j)
        departement = pd.json_normalize(response.json())
        dataset['lat_d'] = np.where(dataset['code']== j, departement['lat'].values, dataset['lat_d'])
        dataset['lon_d'] = np.where(dataset['code']== j, departement['lon'].values, dataset['lon_d'])
    
    return dataset

#dataset = pd.read_csv(final_path)

def create_geodataframe(dataset: pd.DataFrame) -> gpd.GeoDataFrame:
    # Convert the DataFrame to a GeoDataFrame
    geodataframe = gpd.GeoDataFrame(dataset, geometry=gpd.points_from_xy(dataset.lat, dataset.lon))
    
    # Set the coordinate reference system (CRS) to EPSG 4326
    geodataframe.crs = {'init': 'epsg:4326'}
    return geodataframe


def preparation_pipeline():
    dataset = concat_dataset()
    #dataset = pd.read_csv(final_path)
    dataset = create_region(dataset)
    dataset = create_lat_long(dataset)
    dataset = create_code(dataset)
    dataset = create_lat_long_departement(dataset)
    dataset = create_population(dataset)
    save_csv(dataset)
    
    
dataset = concat_dataset()
#dataset = pd.read_csv(final_path)
dataset = create_region(dataset)

dataset = create_lat_long(dataset)

dataset = create_code(dataset)

dataset = create_lat_long_departement(dataset)

dataset = create_population(dataset)
dataset.head()

print(dataset)

preparation_pipeline()


