"""
This function joins all the trolls tweets and returns a dataframe
"""

import pandas as pd
import os

list_of_ids = []
path_of_the_directory = '../data/initial/sixth_data_processing_all_dates_with_trump/'
ext = ('csv')
all_dfs = pd.DataFrame(columns = ['author', 'content', 'language',
       'publish_date', 'hashtags_contained',
       'tweet_id_string', 'external_author_id_string'])

for files in os.scandir(path_of_the_directory):
    if files.path.endswith(ext):
        df = pd.read_csv(files)
        #count += len(df)
        df = df[['author', 'content', 'language',
       'publish_date', 'hashtags_contained',
       'tweet_id_string', 'external_author_id_string']]
        # merge files
        all_dfs = pd.concat([all_dfs,df]) #865102 is total
all_dfs.rename(columns={'author':"troll_name",'tweet_id_string':"referenced_tweets_id",
'content': "troll_tweets", 'language':"troll_language",
       'publish_date':"troll_publish_date", 'hashtags_contained':"troll_hashtags",
                        'external_author_id_string':"troll_id_str"},
               inplace=True)



"""
Save to csv
"""
all_dfs = all_dfs.drop_duplicates(subset=["referenced_tweets_id","troll_publish_date"])
all_dfs = all_dfs.sort_values(by=["referenced_tweets_id","troll_publish_date"])
all_dfs.to_csv("../data/intermediate/all_trolls_tweets.csv",index=None)


