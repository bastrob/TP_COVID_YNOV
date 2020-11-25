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
import emoji

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

# # tweets premier confinement
tweets_confinement_1 = pd.read_csv(final_path_conf_1)

# # tweets deuxieme confinement
tweets_confinement_2 = pd.read_csv(final_path_conf_2)


# 3) Nettoyage du texte, mise en place d'une regex

def load_stop_words() -> List[str]:
    li = []
    with open('stopwords_fr.txt', 'r') as f:
        for line in f:
            li.append(line[:-1])
    return li

stopwords_fr = load_stop_words()    



def extract_emojis(s):
  return ''.join(c for c in s if c in emoji.UNICODE_EMOJI)

def remove_emoji(string):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002500-\U00002BEF"  # chinese char
                               u"\U00002702-\U000027B0"
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               u"\U0001f926-\U0001f937"
                               u"\U00010000-\U0010ffff"
                               u"\u2640-\u2642"
                               u"\u2600-\u2B55"
                               u"\u200d"
                               u"\u23cf"
                               u"\u23e9"
                               u"\u231a"
                               u"\ufe0f"  # dingbats
                               u"\u3030"
                               "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)

text = "#oiuio #lal bonjour remove me http://oko.com https://sz.fr www.loool.kop"
def extract_hashtags(s) :
    li = re.findall(r"#(\w+)", s.lower())
    return " ".join(li)

extract_hashtags(text)

def cleaning_tweet(text):
    """
    # 1] Html tags and attributes (i.e., /<[^>]+>/).
    # 2] Html character codes (i.e., &…;).
    # 3] URLs & Whitespaces.
    
    Returns
    -------
    None.

    """

    # lower text
    text = text.lower()
    
    # remove emojis:
    text = remove_emoji(text)
        
    # Remove #hastag and @user
    text = re.sub("([@#][\w_-]+)", '', text)

    # Remove URL & whitespace
    text = re.sub(r'(http|www)\S+', '', text)
    
    # Remove punctuation & tokenize
    text = [word.strip(string.punctuation) for word in text.split(" ")]

    # Remove stopwords
    stopwords_fr = load_stop_words()
    text = [word for word in text if word not in stopwords_fr and len(word) > 2]

    
    
    return " ".join(text).strip()

def create_important_tweet_col_and_remove_duplicates(dataset: pd.DataFrame) -> pd.DataFrame:
    # counting the duplicates 
    dups = dataset.pivot_table(index = ['tweet_id'], aggfunc ='size') 
    
    dataset = dataset.drop_duplicates(subset="tweet_id")
    dataset.reset_index(drop=True, inplace=True)
    dataset['important_tweet'] = dataset.apply(lambda row: dups[row.tweet_id], axis= 1)
    return dataset

def prepare_dataset(dataset: pd.DataFrame, path_to_save):
    dataset = create_important_tweet_col_and_remove_duplicates(dataset)
    dataset['original_text'] = dataset.text.map(lambda x: x)
    # A value is trying to be set on a copy of a slice from a DataFrame. Try using .loc[row_indexer,col_indexer] = value instead
    dataset.loc[:,'emoji'] = dataset.text.map(lambda x: extract_emojis(x))
    dataset.loc[:, 'hastag'] = dataset.text.map(lambda x: extract_hashtags(x))
    dataset.loc[:, 'text'] = dataset.text.map(lambda x: cleaning_tweet(x))
    save_csv(dataset, path_to_save)
    return dataset

tweets_confinement_1_prepared = prepare_dataset(tweets_confinement_1, final_path_conf_1)
tweets_confinement_2_prepared = prepare_dataset(tweets_confinement_2, final_path_conf_2)




