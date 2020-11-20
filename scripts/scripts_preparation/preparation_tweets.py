# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 10:47:47 2020

@author: Bast
"""

import pandas as pd
import glob
import re
from typing import List
import string

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

def load_stop_words() -> List[str]:
    li = []
    with open('stopwords_fr.txt', 'r') as f:
        for line in f:
            li.append(line[:-1])
    return li

stopwords_fr = load_stop_words()    
print(stopwords_fr)

tweets_confinement_1.head()

print(tweets_confinement_1.info())

tweets_confinement_1.location.value_counts()

import emoji

def extract_emojis(s):
  return ''.join(c for c in s if c in emoji.UNICODE_EMOJI)

s = "🏊‍♀️🥋👟🥎🚴‍♀️⚽️🇫🇷"
print(extract_emojis(s))

text = "#oiuio #lal bonjour remove me http://oko.com https://sz.fr www.loool.kop"
def extract_hashtags(s) :
    li = re.findall(r"#(\w+)", s)
    print(li)

extract_hashtags(text)

def cleaning_tweet():
    """
    # 1] Html tags and attributes (i.e., /<[^>]+>/).
    # 2] Html character codes (i.e., &…;).
    # 3] URLs & Whitespaces.
    
    Returns
    -------
    None.

    """
    text = "#oiuio #lal bonjour et à êtes remove me http://oko.com https://sz.fr www.loool.kop"
    
    # lower text
    text = text.lower()
    
    print(text)
    # Remove #hastag and @user
    text = re.sub("([@#][\w_-]+)", '', text)
    print(text)
    # Remove URL & whitespace
    text = re.sub(r'(http|www)\S+', '', text)
    
    print(text)
    # Remove punctuation & tokenize
    text = [word.strip(string.punctuation) for word in text.split(" ")]
    print(text)
    # Remove stopwords
    stopwords_fr = load_stop_words()
    text = [word for word in text if word not in stopwords_fr]
    print(text)
    
    
    print(' '.join(text))

cleaning_tweet()

print(type(tweets_confinement_1))

def prepare_dataset(dataset):
    dataset['emoji'] = dataset.text.map(lambda x: extract_emojis(x))
    dataset['hastag'] = dataset.text.map(lambda x: extract_hashtags(x))
    dataset['text'] = dataset.text.map(lambda x: cleaning_tweet(x))
    
    return dataset

tweets_confinement_1_prepared = prepare_dataset(tweets_confinement_1)






