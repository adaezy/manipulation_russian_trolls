import pandas as pd
import os
import glob
"""
Function to count the total number of hashtags  whether from missing or not missing
"""

string1 = 'Total Tweets:'

# opening a text file

mydict = {}

def collect_total(path):
    mylist =[]
    for files in os.listdir(path):
        #print(files)
        if files.startswith("output_counts"):
            ind = files.lstrip("output_counts")
            ind = int(ind.rstrip(".txt"))
            with open(path+files, "r") as file1:
                # read file content
                for line in file1:
                    if string1 in line:
                        count = line.lstrip("Total Tweets: ") #21
                        print(ind)
                        print("count",count)
                        count = count.rstrip("\n")
                        mydict[ind] = count
    print(mydict)
    #Add to dataframe and create a csv
    count_df = pd.DataFrame.from_dict(mydict,orient="index")
    count_df.to_csv("../data/processed/" + "counts_" + path[21:-5] + ".csv",header=False)
    # closing a file
    file1.close()

collect_total("../data/intermediate/trolls_hashtag_dir/")
collect_total("../data/intermediate/legitimate_hashtag_dir/")