# -*- coding: utf-8 -*-
"""
Created on Thu Nov  5 08:57:22 2020

@author: Bast
"""

import os
from twython import TwythonStreamer
from collections import Counter
from twython import Twython
from typing import List
import csv
from csv import writer
import json

from datetime import datetime



CONSUMER_KEY = os.environ.get("TWITTER_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("TWITTER_CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")

# import webbrowser
# from twython import Twython

# # Get a temporary client to retrieve an authetification URL
# temp_client = Twython(CONSUMER_KEY, CONSUMER_SECRET)
# temp_creds = temp_client.get_authentication_tokens()
# url = temp_creds["auth_url"]

# # Now visit that URL to authorize the application and get a PIN
# print(f"go visit {url} and get the PIN code and paste it below")
# webbrowser.open(url)
# PIN_CODE = input("please enter the PIN code: ")

# # Now we use that PIN_CODE to get tha actual tokens
# auth_client = Twython(CONSUMER_KEY,
#                       CONSUMER_SECRET,
#                       temp_creds["oauth_token"],
#                       temp_creds["oauth_token_secret"])

# final_step = auth_client.get_authentication_tokens(PIN_CODE)
# ACCESS_TOKEN = final_step["oauth_token"]
# ACCESS_TOKEN_SECRET = final_step["oauth_token_secret"]

# date
# startDate = datetime(2020, 6, 1, 0, 0, 0)
# endDate =   datetime(2020, 1, 1, 0, 0, 0)

# created_at = 'Thu Nov 12 15:00:06 +0000 2020'


# date_string = "21 June, 2018"

# print("date_string =", date_string)
# print("type of date_string =", type(date_string))

# date_object = datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')

# print("date_object =", date_object)
# print("type of date_object =", type(date_object))

# And get a new Twython instance using them
twitter = Twython(CONSUMER_KEY,
                  CONSUMER_SECRET,
                  ACCESS_TOKEN,
                  ACCESS_TOKEN_SECRET)


tweet_count = 0
tracklist = ['#COVID19', '#COVID-19']


class MyStreamer(TwythonStreamer):
    

    def on_success(self, data):
        """
        What do we do when Twitter sends us data?
        Here data will be a Python dict representing a tweet

        Parameters
        ----------
        data : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        filename_csv = 'tweets_stream_dataset_'
        filename_txt = 'tweets_stream_json_'
        format_csv = '.csv'
        format_txt = '.txt'
        count_dataset = 1 
        global tweet_count
        
        full_csv_path = r'..\..\datasets_raw\tweets_datasets\tweets_confinement_2\tweets_stream_csv\{}{}{}'.format(filename_csv, count_dataset, format_csv)
        full_json_path = r'..\..\datasets_raw\tweets_datasets\tweets_confinement_2\tweets_stream_json\{}{}{}'.format(filename_txt, count_dataset, format_txt)
        # Strips the newline character
        
        # We only want to collect french-language tweets:
        if data.get("lang") == "fr":
            #tweets_stream.append(data)
            self.append_tweet_to_json(full_json_path, data)
            bounding_box = "empty"

            print(f"received tweet #{tweet_count}")
            try:
                if data["retweeted_status"]["place"] != None:
                    bounding_box = data["retweeted_status"]["place"]["bounding_box"]
                print(data["retweeted_status"]["place"])
                print(data["retweeted_status"]["lang"])
                
                row = {'tweet_id': data["retweeted_status"]["id"],
                       'date': data["retweeted_status"]["created_at"],
                       'location': data["retweeted_status"]["user"]["location"],
                       'bounding_box': bounding_box,
                       'lang': data["retweeted_status"]["lang"],
                       'text': data["retweeted_status"]["extended_tweet"]["full_text"].replace('\n', ' ') }
                
                self.append_row_to_csv(full_csv_path, row)
                
            except KeyError:
                if data["place"] != None:
                    bounding_box = data["place"]["bounding_box"]
                print(data["lang"])
                row = {'tweet_id': data["id"],
                       'date': data["created_at"],
                       'location': data["user"]["location"],
                       'bounding_box': bounding_box,
                       'lang': data["lang"],
                       'text': data["text"].replace('\n', ' ') }
                
                self.append_row_to_csv(full_csv_path, row)
        
        if tweet_count >=10000:
            self.disconnect()
        
        tweet_count +=1
        
    def on_error(self, status_code, data):
        print(status_code, data)
        self.disconnect()
    

    def append_row_to_csv(self, filename: str, row: dict):
        # open existing file
        with open(filename, 'a+', newline='', encoding='utf-8') as write_obj:
            # Create a writer object from csv module
            dict_writer = csv.DictWriter(write_obj, fieldnames=('tweet_id', 'date', 'location', 'bounding_box', 'lang', 'text'))

            # add row
            dict_writer.writerow(row)

    def append_tweet_to_json(self, filename: str, data):
        with open(filename, 'a+') as f:
           json_object = json.dumps(data, indent = 4) 
           f.write(json_object)





stream = MyStreamer(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# start consuming public statuses that contain the keywords
stream.statuses.filter(track=tracklist)

# if instead we wanted to start we wanted to start consuming a sample of *all* public statuses
# stream.statuses.sample()



# for tweet in tweets_stream:
    
#     #print(tweet)
#     #print(tweet)
    
#     try:
#         print(tweet["retweeted_status"]["extended_tweet"]["full_text"])
#         print(tweet["retweeted_status"]["created_at"])
#         print(tweet["retweeted_status"]["id"])
#         print(tweet["retweeted_status"]["user"]["location"])
#         if tweet["retweeted_status"]["place"] != None:
#             print(tweet["retweeted_status"]["place"]["bouding_box"])
#         print(tweet["retweeted_status"]["place"])
#         print(tweet["retweeted_status"]["lang"])
#     except KeyError:
#         print(tweet["text"])
#         print(tweet["created_at"])
#         print(tweet["id"])
#         print(tweet["user"]["location"])
#         if tweet["place"] != None:
#             print(tweet["place"]["bouding_box"])
#         print(tweet["lang"])
        



# created_at, text, user location retweeted_status 'extended_tweet': {'full_text': bounding box id
