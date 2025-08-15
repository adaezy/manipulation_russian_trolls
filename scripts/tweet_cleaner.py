"""
Here, I removed the hashtag symbol for tweets for sentiments and
left the hashtag symbol for tweets for emotions.

"""
import pandas as pd
import preprocessor as p
import regex as re
import string
import emoji

from nltk.tokenize import TweetTokenizer
"""
This scripts contains functions that are used in the cleaning of the tweets.
Then followed by initial cleaning of the dataframe
"""
PUNCUATION_LIST = list(string.punctuation)
#PUNCUATION_LIST.pop(2)
print(PUNCUATION_LIST)
def preprocess_tweet(row):
    text = row
    p.set_options(p.OPT.URL, p.OPT.MENTION)
    text = p.clean(text)
    return text

def preprocessing_text(text):
    #Make lowercase
    text = text.str.lower()
    #text = text.apply(lambda x: re.sub(r"[^a-z\s\(\-:\)\\\/\];='#]", '', x))
    text = text.apply(lambda x: re.sub(r"[^a-z\s\(\-:\)\\\/\];='#]", '', x))
    return text

def remove_punctuation(word_list):
    """Remove punctuation tokens from a list of tokens"""
    val = [w for w in word_list if w not in PUNCUATION_LIST]
    return "".join(val)

def change_emoji(text):
    val= emoji.demojize(text, delimiters=("", ""))
    return val


#Read in file
final_data = pd.read_csv("../data/processed/final_data_001.csv",dtype=object,lineterminator="\n")

#Clean up tweets
final_data['troll_tweets'] = final_data['troll_tweets'].apply(preprocess_tweet)
final_data['text'] = final_data['text'].apply(preprocess_tweet)

#Change emojis
final_data['troll_tweets'] = final_data['troll_tweets'].apply(change_emoji)
final_data['text'] = final_data['text'].apply(change_emoji)


final_data['troll_tweets'] = preprocessing_text(final_data['troll_tweets'])
final_data['text'] = preprocessing_text(final_data['text'])

final_data.troll_tweets = final_data.troll_tweets.apply(remove_punctuation)
final_data.text = final_data.text.apply(remove_punctuation)



#Save cleaned tweets to files to get emotions

#final_data["troll_tweets"].to_csv("../data/processed/trolls_tweets_only_em.csv",header=None,index=False)
#final_data["text"].to_csv("../data/processed/legitimate_tweets_only_em.csv",header=None,index=False)

#Save cleaned tweets to files to use in collection of features
final_data.to_csv("../data/processed/final_data_002.csv",index=False)