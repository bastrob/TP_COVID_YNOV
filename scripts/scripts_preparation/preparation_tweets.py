# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 10:47:47 2020

@author: Bast
"""

import pandas as pd
import glob

# 1) Recuperation des fichiers tweets
# 2) Construction des datasets
# 3) Extraction du texte avec une regex
# 4) Recuperation des localisations




# 1) Recuperation des fichiers tweets
# 2) Construction d'un unique dataset pour le premier confinement qui contient plusieurs csv qu'il faudra concatener


# Definition des path
final_path_conf_1 = r'..\..\datasets_cleaned\tweets_conf_1.csv'
final_path_conf_2 = r'..\..\datasets_cleaned\tweets_conf_2.csv'
path_tweet_confinement_1 = r'..\..\datasets_raw\tweets_datasets\tweets_confinement_1\tweets_csv'
path_tweet_confinement_2 = r'..\..\datasets_raw\tweets_datasets\tweets_confinement_2\tweets_stream_csv'

def concat_dataset(path, path_to_save):
    
    all_files = glob.glob(path + "/*.csv")
    li = []
    
    if path == path_tweet_confinement_1:
        columns = ['tweet_id', 'date', 'location', 'lang', 'text']
    else:
        columns = ['tweet_id', 'date', 'location', 'bounding_box', 'lang', 'text']
    
    for filename in all_files:
        df = pd.read_csv(filename)
        df.columns = columns
        li.append(df)

 
    frame = pd.concat(li, ignore_index=True)
    save_csv(frame, path_to_save)

def save_csv(dataset, path_to_save):
    dataset.to_csv(path_to_save, index = False, header=True)
    
concat_dataset(path_tweet_confinement_1, final_path_conf_1)
concat_dataset(path_tweet_confinement_2, final_path_conf_2)

# tweets premier confinement
tweets_confinement_1 = pd.read_csv(final_path_conf_1)

# tweets deuxieme confinement
tweets_confinement_2 = pd.read_csv(final_path_conf_2)


# 3) Nettoyage du texte, mise en place d'une regex
tweets_confinement_1.head()

print(tweets_confinement_1.info())