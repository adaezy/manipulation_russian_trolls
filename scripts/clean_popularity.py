import pandas as pd
from clean_up_counts_hashtags import find_missing
from utils.utils import get_integer_from_string


#For Trolls TODO: Change to Function Later
counts_df_hashtags_troll = pd.read_csv("../data/processed/counts_trolls_hashtag.csv",header=None)
counts_df_hashtags_troll= counts_df_hashtags_troll.sort_values(by=[0])


missing_list_troll = find_missing("../data/processed/counts_trolls_hashtag.csv")
dictionary_missing_troll = {}
for i in missing_list_troll:
    dictionary_missing_troll[i] = 0
#Change to dataframe
missing_df = pd.DataFrame(dictionary_missing_troll.items())
#Add to dataframe
counts_df_hashtags_troll = pd.concat([counts_df_hashtags_troll,missing_df])
counts_df_hashtags_troll.reset_index(drop=True,inplace=True)
counts_df_hashtags_troll = counts_df_hashtags_troll.sort_values(by=[0])






#For legitimate
counts_df_hashtags_legit = pd.read_csv("../data/processed/counts_legitimate_hashtag.csv",header=None)
counts_df_hashtags_legit= counts_df_hashtags_legit.sort_values(by=[0])


missing_list_legit = find_missing("../data/processed/counts_legitimate_hashtag.csv")
dictionary_missing_legit = {}
for i in missing_list_legit:
    dictionary_missing_legit[i] = 0
#Change to dataframe
missing_df = pd.DataFrame(dictionary_missing_legit.items())
#Add to dataframe
counts_df_hashtags_legit = pd.concat([counts_df_hashtags_legit,missing_df])
counts_df_hashtags_legit.reset_index(drop=True,inplace=True)
counts_df_hashtags_legit = counts_df_hashtags_legit.sort_values(by=[0])



#Change the counts of hashtag to numbers
counts_df_hashtags_legit[1] = counts_df_hashtags_legit[1].astype(str).apply(get_integer_from_string)
counts_df_hashtags_troll[1] = counts_df_hashtags_troll[1].astype(str).apply(get_integer_from_string)

counts_df_hashtags_troll.to_csv("../data/processed/counts_df_hashtags_trolls_processed.csv",header=False,index=False)
counts_df_hashtags_legit.to_csv("../data/processed/counts_df_hashtags_legit_processed.csv",header=False,index=False)