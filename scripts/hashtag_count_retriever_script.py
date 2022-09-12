import numpy as np
import pandas as pd
from ast import literal_eval
import re
from nltk.corpus import stopwords
from utils.utils import change_to_exec

"""
I used this to create a scripts on how the hashtags will be collected from twitter and where to save.
The input file is the the date of tweet and set of hashtags
"""


all_stop_words = stopwords.words()
all_stop_words = [st.upper() for st in all_stop_words]
def remove_punct(data):
    text = re.sub(r'[^\w\d\s\#]+', '', data)
    return text
#tweet_stop_words = ["RT"]
def format_hashtag(path):
    data_file = pd.read_csv(path,header=None)
    data_file["ind"] = data_file[0]
    data_file = data_file[["ind",1,2]]
    data_file.set_index("ind",inplace=True)

    #print(data_file.columns)
    #print(data_file.head(1))

    #data_file[1] =  data_file[1].map(lambda a: a.replace("'",""))
    hashtags = data_file[2].to_list()
    hash_list = []
    for ind,i in enumerate(hashtags):
        if i is not np.nan:
            i = literal_eval(i)
            list_to_removed = []
            for cnt,word in enumerate(i):
                """
                Cleaned up issues with stop words
                """
                if word.upper() == "RT":
                    i[cnt] = i[cnt].upper()
                    list_to_removed.append("RT")
                if word.upper() in all_stop_words:
                    list_to_removed.append(word)
                if word.startswith("##"):
                    i[cnt] = word[1:]
            for rm in list_to_removed:
                i.remove(rm)

            if len(i) == 1 and i[0] == '#': #index with tag is 1 and 2790
                hash_list.append(" ")


            elif len(i) == 0:
                hash_list.append(" ")


            elif len(i) == 1:
                hash_val = remove_punct(i[0])
                hash_list.append(hash_val)
            else:
                new_val = " OR ".join(i)
                new_val = remove_punct(new_val)
                fin_val = f"({new_val})"
                hash_list.append(fin_val)

    data_file["final_date"] = data_file[1].str[0:10]
    data_file["final_date"] = pd.to_datetime(data_file["final_date"])
    data_file["begin_date"] = data_file["final_date"] - pd.to_timedelta(7, unit='d')

    data_file = data_file[~data_file[2].isnull()]
    data_file["hashtags"] = hash_list
    data_file = data_file[["begin_date","final_date","hashtags"]]
    print(len(data_file))
    data_file = data_file[data_file["hashtags"] != " "]

    #data_file.to_csv("collect_count_hashtags.csv",index=False,header=None)
    #csv calculate count

    def append_new_line(file_name, text_to_append):
        """Append given text as a new line at the end of file"""
        # Open the file in append & read mode ('a+')
        with open(file_name, "a+") as file_object:
            # Move read cursor to the start of file.
            file_object.seek(0)
            # If file is not empty then append '\n'
            data = file_object.read(100)
            if len(data) > 0:
                file_object.write("\n")
            # Append text at the end of file
            file_object.write(text_to_append)


    for ind,rws in data_file.iterrows():
        dt1,dt2,hashtag = rws["begin_date"],rws["final_date"],rws["hashtags"]
        file_name = "../scripts/" + "retrieve_counts_"+ path[21:-4] + ".sh"
        append_new_line(file_name, "twarc2 counts --archive --start-time" + " " + str(dt1)[0:10] + " " +
        "--end-time" + " " + str(dt2)[0:10] + " " + "--text" + " " +  "\"" + hashtag + "\"" + " " + "../data/intermediate/" + path[21:-5] + "_dir/output_counts" + str(ind) + ".txt")

    change_to_exec(file_name)

format_hashtag("../data/intermediate/trolls_hashtags.csv")
format_hashtag("../data/intermediate/legitimate_hashtags.csv")

"""
Next step is run retrieve_counts_trolls_hashtags.sh and retrieve_counts_legitimate_hashtags.sh
"""


# #Fixed and cleaned data for missing indices
# #missing_vals = [135, 285, 871, 1181, 1331, 1919, 1922, 2195, 2304, 3733, 4296, 4766, 7047, 11958, 14912, 15038, 20136, 27064, 32435, 34353]
#
# #new_df_missing = data_file.iloc[missing_vals]
# #print(new_df_missing)
# #new_df_missing.to_csv("missing_counts_hashtags.csv")
#
#
# #Run again for missing
# missing_df = pd.read_csv("missing_counts_hashtags.csv")
#
# missing_df = missing_df.rename(columns={"Unnamed: 0" : "index"})
# missing_df = missing_df.set_index("index")
# missing_df = missing_df.sort_index()
# count = 0
# for ind,rws in missing_df.iterrows():
#     dt1,dt2,hashtag = rws["begin_date"],rws["final_date"],rws["hashtags"]
#     append_new_line("retrieve_counts_hashtags_missing.txt", "twarc2 counts --archive --start-time" + " " + str(dt1)[0:10] + " " +
#     "--end-time" + " " + str(dt2)[0:10] + " " + "--text" + " " +  "\"" + hashtag + "\"" + " " + "output_counts" + str(ind) + ".txt")
#     count += 1
