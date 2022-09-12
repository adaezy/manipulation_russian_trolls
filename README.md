## Modeling the Russian Troll Manipulation as a Spiking Neuron(Ongoing)

### Project Description
We model the interaction between trolls and legitimate users as a spiking neuron model. 
We have the Russian troll dataset which comprises of tweets from trolls who interacted with legitimate users during the 2016 elections.
We also have tweets of Legitimate users who mention the trolls. We assume that every tweet by legitimate users is part of the history. So for a troll A,
with one or more tweets, every prior tweet and responses from legitimate user is a part of the history of subsequent reply of user A.

### How to Run Code- Run in this sequence from the scripts folder
* get_all_trolls_tweets.py
* get_author_ids_from_trolls.py
* russia_trolls_analyis.py
* tweet_cleaner.py
* hashtag_count_retriever_script.py
* retrieve_counts_trolls_hashtags.sh 
* retrieve_counts_legitimate_hashtags.sh
* collect_count_hashtags_from_files.py
* clean_up_counts_hashtags.py(optional,only if missing rows occur)
* clean_popularity.py
* add_all_features.py
* get_history.py
* add_all_history.py

#### Note
When you run the script retrieve_counts_legitimate_hashtags.sh and
retrieve_counts_trolls_hashtags.sh, verify if all the lines of code returned a result using clean_up_counts_hashtags.py.

The bash script are created automatically but relies on twarc library to get data from Twitter.

