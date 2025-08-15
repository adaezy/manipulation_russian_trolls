"""
Do a cummulative by index from the results of the get_history script
"""
import ast
import pandas as pd
import numpy as np

final_data = pd.read_csv("../data/processed/final_data_005.csv",dtype=object,lineterminator="\n")

#For trolls
troll_history = final_data['troll_history_to_use']

troll = ['troll_tweets_Negative', 'troll_tweets_Neutral',
       'troll_tweets_Positive', 'troll_tweets_Length_of_words',
        'troll_hashtag_popularity', 'troll_Anger', 'troll_Disgust',
       'troll_Fear', 'troll_Joy', 'troll_Sadness', 'troll_Surprise','troll_length_history']
troll_data = final_data[troll].copy()
columns = troll_data.columns.to_list()
troll_data[columns] = troll_data[columns].apply(pd.to_numeric)

#Create a troll history dataframe
troll_history_df = pd.DataFrame(columns=columns)

for i in troll_history.to_list():
    i = ast.literal_eval(i)
    selection = troll_data.iloc[i]
    selection = selection[troll]
    len_select = len(i)
    if len_select == 1:
        df_add_troll = selection
    else:
        df_add_troll = selection.sum(axis=0)
        df_add_troll = pd.DataFrame(df_add_troll)
        df_add_troll = df_add_troll.transpose()
    troll_history_df = pd.concat([troll_history_df,df_add_troll])
    troll_history_df.reset_index(inplace=True,drop=True)





#For Legit Users
legit = ['legit_users_Negative', 'legit_users_Neutral',
       'legit_users_Positive','legit_users_Length_of_words','legit_hashtag_popularity',
         'legit_Anger', 'legit_Disgust', 'legit_Fear', 'legit_Joy',
         'legit_Sadness', 'legit_Surprise','legit_length_history'
         ]

user_history = final_data['user_history_to_use']

user_data = final_data[legit].copy()
columns = user_data.columns.to_list()
user_data[columns] = user_data[columns].apply(pd.to_numeric)

#Create a troll history dataframe
user_history_df = pd.DataFrame(columns=columns)

for i in user_history.to_list():
    i = ast.literal_eval(i)
    selection = user_data.iloc[i]
    #print("selection",selection)
    selection = selection[legit]
    len_select = len(i)
    if len_select == 0:
        df_add_user = np.nan
        #print(df_add_user)
    if len_select == 1:
        df_add_user = selection
        #print(df_add_user)
    else:
        df_add_user = selection.sum(axis=0)
        df_add_user = pd.DataFrame(df_add_user)
        df_add_user = df_add_user.transpose()
    user_history_df = pd.concat([user_history_df,df_add_user])
    user_history_df.reset_index(inplace=True, drop=True)

#Merge the two dataframes
history_df = pd.concat([troll_history_df,user_history_df],axis=1)
history_df["date_diff"] = final_data["date_diff"].to_list()
history_df.to_csv("../data/final/final_data_independent.csv",index=False)


#Sum the history of trolls and users #TODO: Why extra columns
merged_history = pd.DataFrame(columns = ["Negative_Sum","Neutral_Sum","Positive_Sum","Length_Sum","Popularity_Sum","Anger_Sum","Disgust_Sum","Fear_Sum","Joy_Sum","Sadness_Sum","Surprise_Sum","Length_History_Sum"])
for i in range(len(user_history_df)):
    new_values = [x + y for x, y in zip(user_history_df.iloc[i].values, troll_history_df.iloc[i].values)]
    new_values = pd.DataFrame(new_values)
    new_values = new_values.transpose()
    new_values = new_values.rename(columns = {0:"Negative_Sum",1:"Neutral_Sum",2: "Positive_Sum",3:"Length_Sum",
                                    4:"Popularity_Sum", 5:"Anger_Sum",6:"Disgust_Sum",7:"Fear_Sum", 8 :"Joy_Sum", 9:"Sadness_Sum",10:"Surprise_Sum",11:"Length_History_Sum"})
    merged_history = pd.concat([merged_history,new_values])
merged_history["date_diff"] = final_data["date_diff"].to_list()
print(merged_history.columns)
merged_history.to_csv("../data/final/final_data_merged.csv",index=False)




