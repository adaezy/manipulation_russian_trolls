"""
Get all the author ids of trolls in data set and save to csv
"""

import os
import pandas as pd
from utils.utils import flatten_list


list_of_ids = []
path_of_the_directory = '../data/initial/sixth_data_processing_all_dates_with_trump/'
ext = ('csv')
for files in os.scandir(path_of_the_directory):
    if files.path.endswith(ext):
        df = pd.read_csv(files)
        list_of_ids.append(df['external_author_id_string'].to_list())
list_of_ids = list(set(flatten_list(list_of_ids)))
print(len(list_of_ids)) #1151

"""
Save to csv
"""
df_trolls_ids = pd.DataFrame(list_of_ids,columns=["trolls"])
df_trolls_ids.to_csv("../data/intermediate/all_trolls_author_id.csv",header=False,index=False)

