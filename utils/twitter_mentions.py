from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import tweepy
import pandas as pd
import string
import os
import psycopg2
import snowflake.connector
import sys
from datetime import datetime
from extract.utils import snowflake_engine_factory, execute_query, execute_dataframe

def get_twitter_mentions():
    # Twitter credentials
    # Obtain them from your twitter developer account

    # Need to add this or another Twitter Developer API Key to SysVars - This is currently my personal API Key
    consumer_key = os.getenv("TWITTER_CONSUMER_KEY")
    consumer_secret = os.getenv("TWITTER_CONSUMER_SECRET")
    access_key = os.getenv("TWITTER_ACCESS_KEY")
    access_secret = os.getenv("TWITTER_ACCESS_KEY")

    # Pass your twitter credentials to tweepy via its OAuthHandler
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    # Create database connection and cursor
    engine = snowflake_engine_factory(os.environ, "TRANSFORMER", "util")
    connection = engine.connect()
    cur = connection.cursor()

    # Create empty dataframe with required columns
    db_tweets = pd.DataFrame(columns = ['username', 'text', 'full_name', 'user_url', 'url', 'retweet_text', 'original_tweet_date', 'retweeted_status', 'retweet_count', 'created_at', 'location', 
                            'followers', 'user_id', 'favorite_count', 'lang', 'verified', 'hashtags', 'following_count', 'is_tweet_reply', 'id', 'longitude_latitude']
                                    )

    # Fetch latest data from existing ANALYTICS.SOCIAL_MENTIONS.TWITTER relation
    query = f'''
    SELECT MAX(CREATED_AT - interval '1 day')::date::varchar AS DATE, 
    MAX(CREATED_AT)::VARCHAR AS TIMESTAMP 
    FROM analytics.social_mentions.twitter
    '''

    try:
        cur.execute(query)
        results = cur.fetchall()
    except Exception as e:
        print(f'''Oh no! There was an error executing your query: {e}''')

    # Retrieve all tweets >= Max Created At in ANALYTICS.SOCIAL_MENTIONS.TWITTER relation
    tweets = tweepy.Cursor(api.search,
    q="mattermost",
    since=f"{results[0][0]}").items(5000)

    # Loop through new tweets and extract relevant fields to populate dataframe.
    for tweet in tweets:
        is_tweet_reply = True if tweet.in_reply_to_screen_name != None else False
        username = tweet.user.screen_name
        full_name = tweet.user.name
        user_url = tweet.user.url
        url = f'https://twitter.com/{tweet.user.screen_name}/status/{tweet.id}'
        retweet_count = tweet.retweet_count
        verified = tweet.user.verified
        user_id = tweet.user.id
        favorite_count = tweet.favorite_count
        acctdesc = tweet.user.description
        location = tweet.user.location
        following = tweet.user.friends_count
        followers = tweet.user.followers_count
        totaltweets = tweet.user.statuses_count
        usercreatedts = tweet.user.created_at
        created_at = tweet.created_at.strftime("%Y-%m-%d %H:%M:%S")
        lang = tweet.lang
        hashtags = str(tweet.entities['hashtags'])
        longitude_latitude = tweet.coordinates
        tweet_id = tweet.id
        
        try:
            text = tweet.text
            retweet_text = tweet.retweeted_status.text
            original_tweet_date = tweet.retweeted_status.created_at.strftime("%Y-%m-%d %H:%M:%S")
            is_retweet = True if tweet.retweeted_status.text != None else False
        except AttributeError:  # Not a Retweet
            text = tweet.text
            original_tweet_date = None
            retweet_text = None
            is_retweet = False
        
        
                            
        # Add variables to tweet list to be inserted into dataframe:
        ith_tweet = [username, text, full_name, user_url, url, retweet_text, original_tweet_date, is_retweet, retweet_count, created_at, location, 
                            followers, user_id, favorite_count, lang, verified, hashtags, following, is_tweet_reply, tweet_id, longitude_latitude]
        # Append to dataframe - db_tweets
        db_tweets.loc[len(db_tweets)] = ith_tweet

    # Append dataframe to ANALYTICS.SOCIAL_MENTIONS.TWITTER relation
    db_tweets[db_tweets['created_at'] > results[0][1]].to_sql('twitter', 
                    con=connection,
                    index=False,
                    schema="SOCIAL_MENTIONS",
                    if_exists="append"
        )

if __name__ == "__main__":
    get_twitter_mentions()