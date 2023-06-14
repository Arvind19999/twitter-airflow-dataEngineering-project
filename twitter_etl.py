import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

    access_key = "pxzsnMKzyV9HDfoqvIsKCDHeK" 
    access_secret = "Q8db7FkT9XxdQvGKQ45DKdNWqGB9Yz7RqWKv1zHCfCiNlDESOU" 
    consumer_key = "1668624471325016064-560B1NQn4yC8XIsjA5sXGDS9XvNqBg"
    consumer_secret = "Cn4h7gQ7zQ6dr0O3Ns6WM5U4dFbMT0xwYCU9UqQGZJ6Lv"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('refined_tweets.csv')
