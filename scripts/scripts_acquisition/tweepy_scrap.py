# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 18:01:52 2020

@author: Bast
"""


import tweepy
import os
import time
import csv
from typing import List
import pandas as pd
import json



CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)

# public_tweets = api.home_timeline()
# for tweet in public_tweets:
#     print(tweet.text)



#status = api.get_status("1240626196754370561", tweet_mode="extended")
# print(status._json)
# json_str = json.dumps(status._json)
# print(json_str)
#print(status)
# print(status.retweeted_status.lang)
# print(status.lang)

def create_csv_file(filename: str):
    with open(filename, 'w') as f:
        csv_writer = csv.DictWriter(f, fieldnames=('tweet_id', 'date', 'location', 'lang', 'text'))
        csv_writer.writeheader()

def append_row_to_csv(filename: str, row: dict):
    # open existing file
    with open(filename, 'a+', newline='', encoding='utf-8') as write_obj:
       # Create a writer object from csv module
        dict_writer = csv.DictWriter(write_obj, fieldnames=('tweet_id', 'date', 'location', 'lang', 'text'))

        # add row
        dict_writer.writerow(row)

def append_tweet_to_json(filename: str, tweet: tweepy.Status):
    with open(filename, 'a+') as f:
        json_object = json.dumps(tweet._json, indent = 4) 
        f.write(json_object)

def on_limit(status: tweepy.Status) -> bool:
    print ("Rate Limit Exceeded, Sleep for 15 Mins")
    time.sleep(15 * 60)
    return True

def on_error(status: tweepy.Status) -> bool:
    is_on_error = True
    try:
        print(status.code)
    except AttributeError:
        is_on_error = False
    
    return is_on_error

def on_status(filename_csv: str, status: tweepy.Status):
    try:
        row = {'tweet_id': status.retweeted_status.id,
               'date': status.retweeted_status.created_at,
               'location': status.retweeted_status.user.location,
               'lang': status.retweeted_status.lang,
               'text': status.retweeted_status.full_text.replace('\n', ' ') }
        append_row_to_csv(filename_csv, row)
        # print(status.retweeted_status.full_text) # Retweet
        # print(status.retweeted_status.created_at)
        # print(status.retweeted_status.id)
        # print(status.retweeted_status.user.location)
        print("-----")
        
    except AttributeError:  # Not a Retweet
        row = {'tweet_id': status.id,
               'date': status.created_at,
               'location': status.user.location,
               'lang': status.lang,
               'text': status.full_text.replace('\n', ' ') }
        append_row_to_csv(filename_csv, row)
        # print(status.full_text)
        # print(status.id)
        # print(status.created_at)
        # print(status.user.location)


        
# for line in list_id:
#     try:
#         status = api.get_status(line, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, tweet_mode="extended")
   
#     except tweepy.error.TweepError as err:
#         print ("Caught TweepError: %s" % (err))
#         print('Traitement Line Error: ', line)
#         continue
#     print('Traitement Line: ', line)
#     on_status(status)

# A chaque changement de fichier: création d'un nouveau csv, et création d'un nouveau json

filename_csv = 'tweets_dataset_'
filename_txt = 'tweets_json_'
format_csv = '.csv'
format_txt = '.txt'
count_dataset = 15


file1 = open("../../LockdownDays/df_idsj27.txt", 'r') 
Lines = file1.readlines() 

full_csv_path = r'..\..\datasets_raw\tweets_datasets\tweets_confinement_1\tweets_csv\{}{}{}'.format(filename_csv, count_dataset, format_csv)
full_json_path = r'..\..\datasets_raw\tweets_datasets\tweets_confinement_1\tweets_json\{}{}{}'.format(filename_txt, count_dataset, format_txt)
# Strips the newline character 
for line in Lines:
    try:
        status = api.get_status(line, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, tweet_mode="extended")

    except tweepy.error.TweepError as err:
        print ("Caught TweepError: %s" % (err))
        print('Traitement Line Error: ', line)
        continue
    print('Traitement Line: ', line)
    on_status(full_csv_path, status)
    append_tweet_to_json(full_json_path, status)


# entries = os.listdir('LockdownDays/')
# print(entries)

# for entry in entries:
#     # Using readlines() 
#     print(entry)
#     full_csv_path = f'{filename_csv}{count_dataset}{format_csv}'
#     full_json_path = f'{filename_txt}{count_dataset}{format_txt}' 
#     #create_csv_file(full_csv_path)
    
#     file1 = open("LockdownDays/"+entry, 'r') 
#     Lines = file1.readlines() 

#     # Strips the newline character 
#     for line in Lines:
#         try:
#             status = api.get_status(line, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, tweet_mode="extended")

#         except tweepy.error.TweepError as err:
#             print ("Caught TweepError: %s" % (err))
#             print('Traitement Line Error: ', line)
#             continue
#         print('Traitement Line: ', line)
#         on_status(full_csv_path, status)
#         append_tweet_to_json(full_json_path, status)
    
#     count_dataset += 1




# read_cs = pd.read_csv(filename_csv)
# read_cs.info()
        

