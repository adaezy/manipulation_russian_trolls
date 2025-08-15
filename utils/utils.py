import functools
import operator

import stat
import os

from distfit import distfit
import numpy as np
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def flatten_list(alist):
    newlist = functools.reduce(operator.iconcat, alist, [])
    return newlist


# def remove_url(tweet):
#     '''
#     Utility function to clean tweet text by removing links, special characters
#     using simple regex statements.
#     '''
#     tweet = re.sub('@[^\s]+','',tweet)
#     tweet = re.sub('http[^\s]+','',tweet)
#     return tweet



def get_sentiments(text_df):
    sid = SentimentIntensityAnalyzer()
    enlist = []
    polarity = sid.polarity_scores(text_df)
    enlist.append(polarity)

    #print(enlist)
    #vals = [sub['compound'] for sub in enlist]
    return [enlist[0]['neg'],enlist[0]['neu'],enlist[0]['pos']]


def get_integer_from_string(val):
    val = int(val.replace(",",""))
    return val



def convert_lambda(val):
    return (val/3600) #corrected data



def change_to_exec(somefile):
    """
    Make a file executable
    """
    st = os.stat(somefile)
    os.chmod(somefile, st.st_mode | stat.S_IEXEC)


def exponential_fit(data):
    # Initialize
    dist_expon = distfit(distr='expon',stats='wasserstein')
    # Fit on data
    dist_expon.fit_transform(np.array(data))
    #dist_expon.plot()
    return dist_expon.summary["scale"].to_list()[0]
