# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 22:06:21 2020

@author: Bast
"""

import pandas as pd
import geopandas as gpd
import folium
from folium import Choropleth, Marker
from folium.plugins import MarkerCluster
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns


def load_csv(path: str) -> pd.DataFrame:
    dataset = pd.read_csv(path)
    return dataset


def create_geodataframe(dataset: pd.DataFrame) -> gpd.GeoDataFrame:
    geodataframe = gpd.GeoDataFrame(dataset, geometry=gpd.points_from_xy(dataset.lon, dataset.lat))
    
    #Set the coordinate reference system (CRS) to EPSG 4326
    geodataframe.crs = {'init': 'epsg:4326'}
    return geodataframe


def create_group_region_by_smth(dataset: gpd.GeoDataFrame, col_choice: str) -> pd.Series:
    """
    Création d'une Séries pour la visualisation

    Parameters
    ----------
    dataset : gpd.GeoDataFrame
        DESCRIPTION.
    col_choice : str
        DESCRIPTION.

    Returns
    -------
    Series pandas.

    """

    return dataset.groupby(['region', 'date', 'lat', 'lon'])[col_choice].sum()
    
    

def create_map_france(dataset: gpd.GeoDataFrame, date_str: str, col_name: str):
    """
    Creation d'une map représentant les régions avec une caractéristique (hospitalisation, décés, etc..)

    Parameters
    ----------
    dataset : gpd.GeoDataFrame
        DESCRIPTION.
    date_str : str
        DESCRIPTION.
    col_name : str
        DESCRIPTION.

    Returns
    -------
    None.

    """
    group_region = create_group_region_by_smth(dataset, col_name)
    
    # Create the map
    m = folium.Map(location=[48.8534,2.3488], tiles='cartodbpositron', zoom_start=6)

    # Add points to the map
    mc = MarkerCluster()
    for idx, row in group_region.iteritems():
        if idx[1] == date_str:
            tamp = 0
            while tamp < row:
                mc.add_child(Marker([idx[2], idx[3]]))
                tamp += 1
    m.add_child(mc)

    # Display the map
    return m
    

def load_contour_region() -> gpd.GeoDataFrame:
    return gpd.read_file('../regions/regions-20180101.shp')

def load_contour_departement() -> gpd.GeoDataFrame:
    return gpd.read_file('../departements/departements-20180101.shp')

def transform_contour_region_to_series() -> pd.Series:
    contour_region = load_contour_region()
    return contour_region[['nom', 'geometry']].set_index('nom')

def create_series_for_map2(series: pd.Series, date_str: str) -> pd.Series:
    my_dict = {}
    for idx, value in series.iteritems():
        if idx[1] == date_str:
            my_dict[idx[0]] = value
        
    return pd.Series(my_dict)
    
     
def create_map_france_2(dataset: gpd.GeoDataFrame, date_str: str, col_name: str):
    """
    Parameters
    ----------
    dataset : gpd.GeoDataFrame
        DESCRIPTION.
    date_str : str
        DESCRIPTION.
    col_name : str
        DESCRIPTION.

    Returns
    -------
    None.

    """
    group_region = create_group_region_by_smth(dataset, col_name)
    my_series = create_series_for_map2(group_region, date_str)
    my_contour_region = transform_contour_region_to_series()
    
    # Create a base map
    m = folium.Map(location=[48.8534,2.3488], tiles='cartodbpositron', zoom_start=6)

    # Add a choropleth map to the base map
    Choropleth(geo_data=my_contour_region.__geo_interface__, 
               data=my_series, 
               key_on="feature.id", 
               fill_color='YlGnBu', 
               legend_name='Hospitalisation (2020-11-03)'
               ).add_to(m)

    # Display the map
    return m

def suppression_date_weird(dataset: pd.DataFrame) -> pd.DataFrame:
    weird_date = ['2020-02-29', '2020-03-01', '2020-03-02',
       '2020-03-13', '2020-02-28', '2020-01-24', '2020-01-25',
       '2020-01-26', '2020-01-27', '2020-01-28', '2020-01-29',
       '2020-01-30', '2020-01-31', '2020-02-03', '2020-02-05',
       '2020-02-06', '2020-02-07', '2020-02-08', '2020-02-10',
       '2020-02-11', '2020-02-12', '2020-02-20', '2020-02-24',
       '2020-02-25', '2020-02-27', '2020-02-26', '2020-03-03',
       '2020-03-04', '2020-03-05']
    
    return dataset[~dataset['date'].isin(weird_date)]

def essai_contour_region(geodataframe: gpd.GeoDataFrame):
    contour_region = load_contour_region()
    # Define a base map with county boundaries
    ax = contour_region.plot(figsize=(10,10), color='none', edgecolor='gainsboro', zorder=3)

    # Add wild lands, campsites, and foot trails to the base map
    return geodataframe.plot(color='lightgreen', ax=ax)
    
def essai_contour_departement(geodataframe: gpd.GeoDataFrame):
    contour_departement = load_contour_departement()
    # Define a base map with county boundaries
    ax = contour_departement.plot(figsize=(10,10), color='none', edgecolor='gainsboro', zorder=3)

    # Add wild lands, campsites, and foot trails to the base map
    return geodataframe.plot(color='lightgreen', ax=ax)    


def create_interactive_bar_chart(dataset: pd.DataFrame, col_choice: str):
    dataset.sort_values("date", axis = 0, ascending = True, 
                  inplace = True)
    # Le mieux serait d'ajouter une colonne mois plutôt que la date jour/jour
    fig1 = px.bar(dataset, x="region", y=col_choice, color="region",
                 animation_frame="date", animation_group="departement", range_y=[0,15000])
    fig1.show()


# Create show_wordcloud method:
def show_wordcloud(data, title):
    text = ' '.join(data['text'].astype(str).tolist())
           
# Create the wordcloud object
    fig_wordcloud = WordCloud(background_color='lightgrey',
                            colormap='viridis', width=800, height=600).generate(text)
    
# Display the generated image:
    plt.figure(figsize=(10,7), frameon=True)
    plt.imshow(fig_wordcloud)
    plt.axis('off')
    plt.title(title, fontsize=20)
    plt.show()
          

def plot_evenements_majeurs(df: pd.DataFrame):
    color = sns.color_palette()
    sns.set_style('darkgrid')
    
    f, ax = plt.subplots(figsize = (15, 10))
    ax = sns.lineplot(x="date", y="hospitalises", data=df, ci=None, estimator=sum, ax=ax);
    
    #Annotation des évènements majeurs
    
    plt.annotate('stade 2 28-02',
                xy=('2020-02-28', 0),
                xycoords='data',
                xytext=(0, 50),
                textcoords='offset points',
                arrowprops=dict(arrowstyle='-', color='black'),
                ha='center',
                va='center')
    
    plt.annotate('premier confinement 14-03',
                xy=('2020-03-14', 12),
                xycoords='data',
                xytext=(0, 100),
                textcoords='offset points',
                arrowprops=dict(arrowstyle='-', color='black'),
                ha='center',
                va='center')
    
    plt.annotate('prolongement du confinement 13-04',
                xy=('2020-04-13', 30580),
                xycoords='data',
                xytext=(0, 50),
                textcoords='offset points',
                arrowprops=dict(arrowstyle='-', color='black'),
                ha='center',
                va='center')
    
    plt.annotate('debut du déconfinement progressif 11-05',
                xy=('2020-05-11', 21283),
                xycoords='data',
                xytext=(0, 50),
                textcoords='offset points',
                arrowprops=dict(arrowstyle='-', color='black'),
                ha='center',
                va='center')
    
    plt.annotate('seconde phase du déconfinement 02-06',
                xy=('2020-06-02', 13352),
                xycoords='data',
                xytext=(0, 50),
                textcoords='offset points',
                arrowprops=dict(arrowstyle='-', color='black'),
                ha='center',
                va='center')
    
    plt.annotate('troisième phase du déconfinement 22-06',
                xy=('2020-06-22', 9128),
                xycoords='data',
                xytext=(0, 50),
                textcoords='offset points',
                arrowprops=dict(arrowstyle='-', color='black'),
                ha='center',
                va='center')
    
    plt.annotate('port du masque dans les entreprises 09-01',
                xy=('2020-09-01', 4277),
                xycoords='data',
                xytext=(0, 50),
                textcoords='offset points',
                arrowprops=dict(arrowstyle='-', color='black'),
                ha='center',
                va='center')
    
    plt.annotate('second confinement 01-11',
                xy=('2020-11-01', 23253),
                xycoords='data',
                xytext=(0, 50),
                textcoords='offset points',
                arrowprops=dict(arrowstyle='-', color='black'),
                ha='center',
                va='center')
    
    plt.show()

def calcul_pourcentage_population(dataset: pd.DataFrame, date: str, col_choice: str) -> pd.DataFrame :
    group_region = dataset.groupby(['region', 'date', 'population_region'])[col_choice].sum()
    my_dict = {}
    region = []
    population_region = []
    pourcentage = []
    for idx, row in group_region.iteritems():
        if idx[1] == date:
            region.append(idx[0])
            population_region.append(idx[2])
            pourcentage.append((row / idx[2]) * 100)

    my_dict['region'] = region
    my_dict['population_region'] = population_region
    my_dict['pourcentage_' + col_choice] = pourcentage
    
    return pd.DataFrame.from_dict(my_dict)
    