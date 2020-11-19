# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 21:08:25 2020

@author: Bast
"""

#https://github.com/florianzemma/CoronavirusAPI-France

import requests 
import csv

URL_LIVE = "https://coronavirusapi-france.now.sh/AllLiveData"

# error 503:
URL_ALL = "https://coronavirusapi-france.now.sh/AllData"
URL_DP_EMPTY= "https://coronavirusapi-france.now.sh/AllDataByDepartement?Departement="

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

# r = requests.get(url="https://coronavirusapi-france.now.sh/AllDataByDepartement?Departement=Côtes-d'Armor")
# print(r.status_code)
# data = r.json()
# print(data)

def delete_old_csv(filename: str):
    with open(filename, 'w', encoding='utf-8'):
        pass
        

def append_row_to_csv(filename: str, row: dict):
    # open existing file
    with open(filename, 'a+', newline='', encoding='utf-8') as write_obj:
        # Create a writer object from csv module
        dict_writer = csv.DictWriter(write_obj, fieldnames=('departement', 'date', 'hospitalises', 'reanimations',
                                                            'nvx_hospitalises', 'nvlles_reanimations', 'gueris', 'deces'))
        
        # add row
        dict_writer.writerow(row)


folder = '..\..\datasets_raw\covid_departements_datasets\\'
filename_csv = 'evolution_covid_'
format_csv = '.csv'
print(r'{}{}{}'.format(folder, filename_csv, format_csv))

#col: dep, date, hospitalises, reanimation, nvx_hospitalises, nvx_reanimations, deces, gueris



for departement in departements:
    URL_DEPARTEMENT = f'{URL_DP_EMPTY}{departement}'
    r = requests.get(url= URL_DEPARTEMENT)
    
    data = r.json()
    filename = r'{}{}{}{}'.format(folder, filename_csv, departement, format_csv)
    delete_old_csv(filename)
    
    for value in data.values():
        for element in value:
            # Prendre en compte que certaines clés n'existent pas pour les premiers jours du confinement:
            # ex: nouvellesHospitalisations etc ..
            row = {
                'departement' : departement,
                'date' : element.get('date'),
                'hospitalises' : element.get('hospitalises', 0),
                'reanimations' : element.get('reanimation', 0),
                'nvx_hospitalises' : element.get('nouvellesHospitalisations', 0),
                'nvlles_reanimations' : element.get('nouvellesReanimations', 0),
                'gueris' : element.get('gueris', 0),
                'deces' : element.get('deces', 0)
                }
            
            #filename = f'{folder}{filename_csv}{departement}{format_csv}'
            append_row_to_csv(filename, row)
        
    print("Processed " + departement)
    