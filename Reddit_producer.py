import csv
import praw
import json
from kafka import KafkaProducer
import configparser
from datetime import datetime
import threading

threads = []
top_subreddit_list = ['AskReddit', 'politics', 'Cricket', 'olympics', 'news']


# Initialize Reddit API client
reddit = praw.Reddit(
    client_id='JKqYO7s9Cv3wbCnfsW_C0A',
    client_secret='7X91IaxX0BUrmlX9HZyAohEp6PlmdQ',
    user_agent='Karan'
)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def start_stream(subreddit_name) -> None:
    subreddit = reddit.subreddit(subreddit_name)
    # comment: praw.models.Comment
    for comment in subreddit.stream.comments(skip_existing=True):
        try:
            comment_json = {
                "id": comment.id,
                "body": comment.body,
                "subreddit": comment.subreddit.display_name,
                "timestamp": comment.created_utc
            }
            producer.send("redditcomments", value=comment_json)
            print(f"subreddit: {subreddit_name}, comment: {comment_json}")
        except Exception as e:
            print("An error occurred:", str(e))

def start_streaming_threads():
    for subreddit_name in top_subreddit_list:
        thread = threading.Thread(target=start_stream, args=(subreddit_name,))
        thread.start()
        threads.append(thread)

    for thread in threads:
            thread.join()


start_streaming_threads()
