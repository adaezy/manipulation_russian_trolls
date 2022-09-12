import pandas as pd
from utils.utils import get_sentiments,convert_lambda



final_data = pd.read_csv("../data/processed/final_data_002.csv",dtype=object,lineterminator="\n")

#Get the missing rows and drop them
rows_with_nans = final_data[final_data["text"].isnull()].index.to_list() #TODO Drop this row in hashtags and emotions when imported
final_data = final_data[~final_data["text"].isnull()]
final_data.reset_index(inplace=True,drop=True)

#Get sentiments
final_data["troll_sentiment"] = final_data["troll_tweets"].apply(get_sentiments)
final_data["legit_sentiment"] = final_data["text"].apply(get_sentiments)

final_data[['troll_tweets_Negative','troll_tweets_Neutral','troll_tweets_Positive']] = pd.DataFrame(final_data.troll_sentiment.tolist(),index= final_data.index)
final_data[['legit_users_Negative','legit_users_Neutral','legit_users_Positive']] = pd.DataFrame(final_data.legit_sentiment.tolist(),index= final_data.index)

#Get length
final_data["troll_tweets_Length_of_words"] = final_data['troll_tweets'].apply(lambda x: len(x.split()))
final_data["legit_users_Length_of_words"] = final_data['text'].apply(lambda x: len(x.split()))


#Get popularity
#Drop rows with nans  using index from legitimate tweets
troll_hashtag_popularity = pd.read_csv("../data/processed/counts_df_hashtags_trolls_processed.csv",header=None)[1]
troll_hashtag_popularity = troll_hashtag_popularity[~troll_hashtag_popularity.index.isin(rows_with_nans)]
troll_hashtag_popularity.reset_index(inplace=True,drop=True)
final_data["troll_hashtag_popularity"] = troll_hashtag_popularity


#Drop rows with nans in all legitimate data
legit_hashtag_popularity = pd.read_csv("../data/processed/counts_df_hashtags_legit_processed.csv",header=None)[1]
legit_hashtag_popularity = legit_hashtag_popularity[~legit_hashtag_popularity.index.isin(rows_with_nans)]
legit_hashtag_popularity.reset_index(inplace=True,drop=True)
final_data["legit_hashtag_popularity"] = legit_hashtag_popularity




#Get emotions
troll_emotions = pd.read_csv("../data/processed/trolls_emotions.csv",header=None)
#Drop rows with nans
troll_emotions = troll_emotions[~troll_emotions[0].isin(rows_with_nans)]
troll_emotions.reset_index(inplace=True,drop=True)
#One hot encoding of Trolls Emotions
trolls_emotion_one_hot = pd.get_dummies(troll_emotions[1])
final_data = final_data.join(trolls_emotion_one_hot)
final_data.rename(columns ={'Anger':"troll_Anger", 'Disgust':'troll_Disgust', 'Fear':'troll_Fear','Joy':'troll_Joy', 'Sadness':'troll_Sadness', 'Surprise':'troll_Surprise'},inplace=True)


#Get legit emotions
legit_emotions = pd.read_csv("../data/processed/legit_emotions.csv",header=None)
legit_emotions.reset_index(inplace=True,drop=True)
#One hot encoding of Legit users  Emotions
legit_emotion_one_hot = pd.get_dummies(legit_emotions)
final_data = final_data.join(legit_emotion_one_hot)
final_data.rename(columns ={'1_Anger':"legit_Anger", '1_Disgust':'legit_Disgust', '1_Fear':'legit_Fear','1_Joy':'legit_Joy', '1_Sadness':'legit_Sadness', '1_Surprise':'legit_Surprise'},inplace=True)



#Group by troll_id,referenced_id and time
#Get lambda
columns = [ 'created_at', 'troll_publish_date',
                     'troll_id_str_x',         'referenced_tweets_id',
              'troll_tweets_Negative',         'troll_tweets_Neutral',
              'troll_tweets_Positive',         'legit_users_Negative',
                'legit_users_Neutral',         'legit_users_Positive',
       'troll_tweets_Length_of_words',  'legit_users_Length_of_words',
           'troll_hashtag_popularity',     'legit_hashtag_popularity',
                        'troll_Anger',                'troll_Disgust',
                         'troll_Fear',                    'troll_Joy',
                      'troll_Sadness',               'troll_Surprise',
                                          'legit_Anger',
                      'legit_Disgust',                   'legit_Fear',
                          'legit_Joy',                'legit_Sadness',
                     'legit_Surprise']
final_data = final_data[columns]

final_data.to_csv("../data/processed/final_data_003.csv",sep=",",index=False)

