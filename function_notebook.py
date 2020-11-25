# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 22:06:21 2020

@author: Bast
"""

import pandas as pd
import geopandas as gpd
import folium
from folium import Choropleth, Circle, Marker
from folium.plugins import HeatMap, MarkerCluster

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
    m
    

def load_contour_region() -> gpd.GeoDataFrame:
    return gpd.read_file('../regions/regions-20180101.shp')

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
    m
    
    