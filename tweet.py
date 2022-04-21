import tweepy
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from dotenv import load_dotenv
import os
import json

# twitter auth
load_dotenv()
bearer_token = os.getenv("BEARER_TOKEN")

# google auth
project_id = os.getenv("PROJECT_ID")
key_filename = os.getenv("KEY_FILENAME")
key_path = f"{key_filename}.json"
credentials = service_account.Credentials.from_service_account_file(
key_path,
scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = client.topic_path(project_id, 'tweets')

class SimpleStreamListener(tweepy.StreamingClient):
    def on_tweet(self, tweet):
        print(f"{tweet.id} {tweet.created_at} ({tweet.author_id}): {tweet.text}")
        print("-"*50)
        tweet_data = json.dumps({'id': tweet.id, 'created_at': tweet.created_at, 'text': tweet.text}, default=str)
        client.publish(topic_path, data=tweet_data.encode('utf-8'))
    
    def on_error(self, status):
        print(status)
        if status == 420:
            return False

stream_listener = SimpleStreamListener(bearer_token)

# add new rules    
rule = tweepy.StreamRule(value="data")
stream_listener.add_rules(rule)

stream_listener.filter(expansions="author_id",tweet_fields="created_at")