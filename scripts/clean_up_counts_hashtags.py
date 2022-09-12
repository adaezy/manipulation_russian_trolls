"""
This script is to find missing rows from the hashtag count we got from Twitter through twarc.
"""
import pandas as pd

def find_missing(file):
    hashtag = pd.read_csv(file,dtype=object,header=None)
    hashtag[0] = hashtag[0].astype(int)
    hashtag = hashtag.sort_values(by=[0],ascending=True)

    hashtag_index = hashtag[0].to_list()

    #Find the missing index
    minimum = hashtag_index[0]
    maximum = hashtag_index[-1]

    missing_list = []
    for ind in range(minimum,maximum+1):
        if ind not in hashtag_index:
            missing_list.append(ind)
    return missing_list

#print(find_missing("../data/processed/counts_trolls_hashtag.csv")) # [1109]
#missing = find_missing("../data/processed/counts_legitimate_hashtag.csv")
#[8, 31, 66, 67, 68, 104, 108, 113, 183, 232, 296, 301, 342, 357, 440, 467, 468, 880, 1016, 1017,
# 1020, 1021, 1022, 1222, 1232, 1254, 1262, 1290, 1377, 1379, 1383, 1415, 1441, 1442, 1444, 1445,
# 1446, 1447, 1450, 1451, 1452, 1453, 1496, 1503, 1517, 1518, 1521, 1535, 1674, 1742, 1822, 1833,
# 1842, 1869, 1976, 2847, 2849, 2881, 3448, 3538, 3592]


