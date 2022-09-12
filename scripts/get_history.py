import time
from datetime import datetime
import pandas as pd
import numpy as np

final_data = pd.read_csv("../data/processed/final_data_003.csv",dtype=object,lineterminator="\n")
final_data["transformed_date"] = np.nan
final_data["date_diff"] = np.nan
history_df_subset =  final_data[["troll_id_str_x","troll_publish_date","created_at"]]
all_groups = dict(tuple(final_data.groupby('troll_id_str_x')))
keys_group = list(all_groups.keys())

columns_add = final_data.columns.to_list()
create_new_data = pd.DataFrame(columns=columns_add)


for i in keys_group:
    various_grps = all_groups[i]
    #concat by groups
    create_new_data = pd.concat([create_new_data,various_grps])


def binary_search(arr, val):
    start = 0
    end = len(arr) - 1
    # Traverse the search space
    while start <= end:

        mid = (start + end) // 2

        if arr[mid] == val:
            return mid

        elif arr[mid] < val:
            start = mid + 1
        else:
            end = mid - 1

    # Return the insert position
    return end + 1


# Create two dictionaries for troll history and user history
troll_history_dict = {}
user_history_dict = {}



history_df_subset["troll_publish_date"] = pd.to_datetime(history_df_subset["troll_publish_date"])
history_df_subset["created_at"] = pd.to_datetime(history_df_subset["created_at"])

for i in keys_group:
    various_grps = all_groups[i]
    if len(various_grps) > 1:
        #Find the troll history
        troll_group = various_grps.sort_values(by=["troll_publish_date"])
        # get the index of the sorted data
        time_troll = troll_group.troll_publish_date.to_list()
        index_troll = troll_group.index.to_list()

        time_users = troll_group.created_at.to_list()
        index_users = troll_group.index.to_list()

        len_users = len(index_users)
        for user in range(len_users):
            user_interested = time_users[user]
            # Use a binary search to do this
            position = binary_search(time_troll, user_interested)
            # get all the index before this point
            ind_list = index_troll[:position]
            troll_history_dict[index_users[user]] = ind_list



        # Find the user history
        user_group = various_grps.sort_values(by=["created_at"])
        index_users_new = user_group.index.to_list()
        time_users_new = user_group.created_at.to_list()

        # Find the current user by id
        for user in range(len_users):
            user_interested = time_users_new[user]
            pos = binary_search(time_users_new, user_interested)
            # Add all history of users before this current user
            ind_list_new = index_users_new[:pos]
            user_history_dict[index_users_new[user]] = ind_list_new
    else:
        #use only troll history
        ind = various_grps.index[0]
        troll_history_dict[ind] =[ind]
        user_history_dict[ind] = []



time_before_tweet = {}
#Get the tweet time before current user's tweet.
for i in keys_group:
    merged_list = []
    various_grps = all_groups[i]
    first = various_grps.sort_values(by=["troll_publish_date"])
    time_troll = various_grps.troll_publish_date.to_list()

    second = various_grps.sort_values(by=["created_at"])
    time_user = various_grps.created_at.to_list()
    index_user = various_grps.index.to_list()
    # merge two sorted list
    merged_list = list(set(merged_list))
    merged_list = sorted(time_troll + time_user)
    #merged_list = sorted(merged_list, key=lambda t: datetime.strptime(t[0], '%Y/%m/%d %H:%M:%S'))

    for user,ind in zip(time_user,index_user):
        #find the time of interest
        time_before = merged_list.index(user) - 1
        #print(merged_list[merged_list.index(user)],merged_list[time_before])
        time_before_tweet[ind] = merged_list[time_before]
        #print(" ")

# print(len(troll_history_dict))
# print(len(user_history_dict ))
# print(len(time_before_tweet))



#create_new_data["user_history_to_use"]


troll_history = pd.DataFrame(troll_history_dict.items())
troll_history.set_index(0,inplace=True)

user_history = pd.DataFrame(user_history_dict.items())
user_history.set_index(0,inplace=True)

time_before = pd.DataFrame(time_before_tweet.items())
time_before.set_index(0,inplace=True)

#Add the troll history
create_new_data["troll_history_to_use"] = troll_history[1].to_list()

#Add the user history
create_new_data["user_history_to_use"] = user_history[1].to_list()


#Add the time difference
create_new_data["time_before_tweet"] = time_before[1].to_list()
create_new_data["time_before_tweet"]  = pd.to_datetime(create_new_data["time_before_tweet"])
create_new_data["created_at"]  = pd.to_datetime(create_new_data["created_at"])
create_new_data["date_diff"] = ((create_new_data["created_at"] - create_new_data["time_before_tweet"])).dt.total_seconds()
create_new_data.sort_index(inplace=True)
create_new_data.to_csv("../data/processed/final_data_005.csv",index=False)