import tweepy
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import os
import json

# twitter auth
load_dotenv()
bearer_token = os.getenv("BEARER_TOKEN")

class SimpleStreamListener(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        print(f"{tweet.id} {tweet.created_at} ({tweet.author_id}): {tweet.text}")
        print("-"*50)
    
    def on_error(self, status):
        print(status)
        if status == 420:
            return False

stream_listener = SimpleStreamListener(bearer_token)

# add new rules    
rule = tweepy.StreamRule(value="data")
stream_listener.add_rules(rule)

stream_listener.filter(expansions="author_id",tweet_fields="created_at")