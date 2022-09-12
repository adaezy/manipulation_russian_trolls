import pandas as pd
import os
import pymongo
import pyarrow
import numpy as np
import bisect
from distfit import distfit

"""
Gets tweets  of legitimate users from mongo db and formats to way we can use.
"""

def get_from_mongo(dbname, col):
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client[dbname]
    collections = db[col]
    data_field = collections.find()
    return data_field

def create_df_tweets(db_name, collection_name):
    results = get_from_mongo(db_name, collection_name)
    user_list = []
    for i in results:
        if "data" in i.keys():
            data_field = i["data"]
            user_dict = {}
            for j in data_field:
                user_dict["created_at"] = j["created_at"] #check
                user_dict["conversation_id"] = j["conversation_id"] #check
                user_dict["author_id"] = str(j["author_id"]) #check
                user_dict["id"] = j["id"] #check
                user_dict["text"] = j["text"] #check
                if "referenced_tweets" in j.keys():
                    user_dict["referenced_tweets_id"] = []
                    length_ref = len(j["referenced_tweets"])
                    #ref_dict = {}
                    for k in range(0,length_ref):
                        #ref_dict["referenced_tweets_type"] = j["referenced_tweets"][k]["type"]
                        #ref_dict["referenced_tweets_id"] = j["referenced_tweets"][k]["id"]
                        user_dict["referenced_tweets_id"].append(j["referenced_tweets"][k]["id"]) #check, confirm it collects multiple values, it
                        # does hence this a list of dictionaries.
                if "in_reply_to_user_id" in j.keys():
                    user_dict["in_reply_to_user_id"] = str(j["in_reply_to_user_id"]) #check
                if "entities" in j.keys():
                    if "hashtags" in j["entities"]:
                        user_dict["hashtags"] = []
                        length_hashtag = len(j["entities"]["hashtags"])
                        for l in range(0, length_hashtag):
                            user_dict["hashtags"].append(j["entities"]["hashtags"][l]["tag"]) #check
                    if "mentions" in j["entities"]:
                        user_dict["mentions"] = []
                        length_mentions = len(j["entities"]["mentions"])
                        for l in range(0, length_mentions):
                            user_dict["mentions"].append(j["entities"]["mentions"][l]["id"]) #check
                if "retweeted_user_id" in j.keys():
                    user_dict["retweeted_user_id"] = j["retweeted_user_id"]
                if "quoted_user_id" in j.keys():
                    user_dict["quoted_user_id"] = j["quoted_user_id"]
                if "lang" in j.keys():
                    user_dict["lang"] = j["lang"] #check
                ###TODO: 'author.id',author.username', 'author.name', work on this
                if "author" in j.keys():
                    user_dict["author.id"] = str("author.id")
                    user_dict["author.username"]= "author.username"
                    user_dict["author.name"] = "author.name"
            user_list.append(user_dict)
    russia_bots_df = pd.DataFrame(user_list)
    return russia_bots_df

"""
Get the dataframes from mongo and merge together
"""
tweets_set1 = create_df_tweets("Russian_trolls", "tweets1")
tweets_set2 = create_df_tweets("Russian_trolls", "tweets2")
tweets_set3 = create_df_tweets("Russian_trolls", "tweets3")
tweets_set4 = create_df_tweets("Russian_trolls", "tweets4")
tweets_set5 = create_df_tweets("Russian_trolls", "tweets5")

legitimate_user_df = pd.concat([tweets_set1, tweets_set2])
legitimate_user_df = pd.concat([legitimate_user_df, tweets_set3])
legitimate_user_df = pd.concat([legitimate_user_df, tweets_set4])
legitimate_user_df = pd.concat([legitimate_user_df, tweets_set5])
legitimate_user_df.reset_index(inplace=True, drop=True)

#Save this data for seperate analysis
mask = (legitimate_user_df["created_at"] >= "2012-02-08T12:14:17.000Z") & (legitimate_user_df["created_at"] <= '2018-05-30T23:59:59.000Z')
legitimate_user_df = legitimate_user_df.loc[mask]

# #print(legitimate_user_df["author_id"].nunique())
# #print(legitimate_user_df['in_reply_to_user_id'].nunique())
legitimate_user_df["author_id"] = legitimate_user_df["author_id"].astype(object)
legitimate_user_df["author_id"] = legitimate_user_df["in_reply_to_user_id"].astype(object)
legitimate_user_df = legitimate_user_df[legitimate_user_df["lang"].isin(["en","und"])]
legitimate_user_df.to_csv("../data/initial/legitimate_users_tweets.csv",index=False)

"""
Step 2:
Changed the focus of research to the reply to direct trolls only. Check for reply to trolls.
We can look at mentions later.
"""

# #uncomment later
"""
Get the author id of all trolls from file all_trolls_author_id.csv
"""
all_trolls = pd.read_csv("../data/intermediate/all_trolls_author_id.csv",header=None)#TODO, will this change my result
all_trolls = all_trolls[0].to_list()
all_trolls_str = []
for id in all_trolls:
    id = id.replace("'","")
    all_trolls_str.append(id)


#Drop rows without referenced tweets
legitimate_user_df = legitimate_user_df.dropna(subset=["referenced_tweets_id", "in_reply_to_user_id"])

#Check if the users in the legitmate datframe is in set of all trolls. save index and ids when found in lists
index_list = []
user_ids_list = []
all_trolls_str_set = set(all_trolls_str)
for ind, rws in legitimate_user_df.iterrows():
    if rws["in_reply_to_user_id"] in all_trolls_str_set:
        index_list.append(ind)
        user_ids_list.append(rws["in_reply_to_user_id"])
#print(len(index_list)) #correct
#print(len(user_ids_list))#correct


#subset by index from index_list
legitimate_user_df_subset = legitimate_user_df.loc[index_list]

#join new columns
legitimate_user_df_subset["troll_id_str"] = user_ids_list
legitimate_user_df_subset = legitimate_user_df_subset.reset_index(drop=True)
#print(russia_bots_df_subset)

"""
Explode the dataframe using referenced_tweets_id
"""
legitimate_user_df_subset_explode= legitimate_user_df_subset.explode("referenced_tweets_id")
legitimate_user_df_subset_explode.to_csv("../data/intermediate/legitimate_user_df_subset_explode.csv",index=False)
#print(russia_bots_df_subset_explode) # 3487 after exploding


"""
Get trolls full tweets and merge on referenced_id_string. Ensure that column names are different.
I have written a function for this already.
"""
"""
Remove the string issues in "author_id_str" and "referenced_tweets_id"
"""
#Here reference tweets id is the real tweet id. Changed for easy reference.
all_trolls_tweets = pd.read_csv("../data/intermediate/all_trolls_tweets.csv",dtype=object)
all_trolls_tweets["troll_id_str"] =  all_trolls_tweets["troll_id_str"].map(lambda a: a.replace("'",""))
all_trolls_tweets["referenced_tweets_id"] =  all_trolls_tweets["referenced_tweets_id"].map(lambda a: a.replace("'",""))

#Merge function
"""
Create a Hash Map of Trolls DataSet
"""
#Change the type of dates in the legitmate users df and trolls df
legitimate_user_df_subset_explode["created_at"] = pd.to_datetime(legitimate_user_df_subset_explode["created_at"],utc=True)
print(legitimate_user_df_subset_explode["created_at"].dtype)
all_trolls_tweets["troll_publish_date"] = pd.to_datetime(all_trolls_tweets["troll_publish_date"],utc=True)
print(all_trolls_tweets["troll_publish_date"].dtype)

#Using a dictionary, save the published date of the trolls tweets together with the referenced tweets id
dictionary_trolls = {}
for ind, rws in all_trolls_tweets.iterrows():
    referenced_tweets_id = rws["referenced_tweets_id"]
    publish_date = rws["troll_publish_date"]
    if referenced_tweets_id in dictionary_trolls:
        dictionary_trolls[referenced_tweets_id].append(publish_date)
    else:
        dictionary_trolls[referenced_tweets_id] = [publish_date]
#print(dictionary_trolls)

#I am using the list to add the published date of the trolls tweets to the dataframe of legitmate users
#To do this , I sort the dates of tweets of the trolls, insert the dates of the legitmate user and
#select the date before the inserted dates

legit_users_date = []
for id, rw in legitimate_user_df_subset_explode.iterrows():
    legit_tweets_id = rw["referenced_tweets_id"]
    legit_date = rw["created_at"]
    if legit_tweets_id in dictionary_trolls.keys():
        add_sort = []
        new_sort = sorted(dictionary_trolls[legit_tweets_id])
        add_sort = new_sort
        #print(add_sort)
        bisect.insort(add_sort,legit_date)
        #print(add_sort)
        index_interest = add_sort.index(legit_date) - 1
        #print(index_interest)
        if index_interest > -1:
            date_interest = add_sort[index_interest]
        else:
            date_interest = -1
        legit_users_date.append(date_interest)
    else:
        date_interest = -1
        legit_users_date.append(date_interest)

#print(legit_users_date)
#print(len(legit_users_date))
#print(len(russia_bots_df_subset_explode)) #9615

#Append new columns to legit users and columns with new dates as -1
legitimate_user_df_subset_explode["ref_troll_publish_date"] = legit_users_date

indexValues = legitimate_user_df_subset_explode[legitimate_user_df_subset_explode["ref_troll_publish_date"] == -1].index
legitimate_user_df_subset_explode = legitimate_user_df_subset_explode.drop(indexValues)
legitimate_user_df_subset_explode["ref_troll_publish_date"] = pd.to_datetime(legitimate_user_df_subset_explode["ref_troll_publish_date"],utc=True)
print(len(legitimate_user_df_subset_explode))

#Then merge the legitimate users to all trolls tweets
trolls_plus_legit = pd.merge(legitimate_user_df_subset_explode,all_trolls_tweets, left_on=["referenced_tweets_id","ref_troll_publish_date"],right_on=["referenced_tweets_id","troll_publish_date"])
trolls_plus_legit.to_csv("../data/intermediate/trolls_plus_legit.csv",index=False)

#TODO --- Get  troll_publish_date and hashtag = sub_all_trolls
trolls_plus_legit[["troll_publish_date","troll_hashtags"]].to_csv("../data/intermediate/trolls_hashtags.csv",header=None)
trolls_plus_legit[["created_at","hashtags"]].to_csv("../data/intermediate/legitimate_hashtags.csv",header=None)


"""
Groupby referenced_tweets_id
"""
# count = 0
all_groups = trolls_plus_legit.groupby("referenced_tweets_id")["text",'created_at',"troll_publish_date","troll_tweets","troll_id_str_x","referenced_tweets_id"] #3780

#print all group names
group_keys= list(all_groups.groups.keys())
print("total_groups",len(group_keys)) #total_groups


list_tweet_ids =[]
list_dates = []

new_gp_df = pd.DataFrame(columns=["text",'created_at',"troll_publish_date","troll_tweets","troll_id_str_x","referenced_tweets_id"])
for gp,vals in all_groups:
    groups_df = all_groups.get_group(gp)
    groups_df.sort_values(by=["troll_publish_date","created_at"],inplace=True)#TODO:figure sorting-use time(40)
    new_gp_df = pd.concat([new_gp_df,groups_df])
print("Length of transformed",len(new_gp_df)) #3681
new_gp_df.to_csv("../data/processed/final_data_001.csv",sep=",",index=False)



#TODO,here I am going to change this code so that I include all tweets of legitmate users in subsequent rows.
# This will be done for the same troll id string. It will help me to add them as part of the history.
#new_gp_df = new_gp_df.drop_duplicates(subset=["referenced_tweets_id","troll_id_str_x"])









