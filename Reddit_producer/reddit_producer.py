import praw
import json
from kafka import KafkaProducer
import threading

# Subreddit set
subreddits = ["AskReddit", "politics", "Cricket", "olympics", "news"]

# Initialize Reddit API client
reddit = praw.Reddit(
    client_id='***************',
    client_secret='*******************',
    user_agent='Karan'
)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def stream_comments(subreddit_name):
    subreddit = reddit.subreddit(subreddit_name)
    print(f"Starting comment stream for subreddit: {subreddit_name}")
    
    for comment in subreddit.stream.comments(skip_existing=True):
        try:
            comment_json = {
                "id": comment.id,
                "body": comment.body,
                "subreddit": comment.subreddit.display_name,
                "timestamp": comment.created_utc
            }
            
            producer.send("redditcomments", value=comment_json)
            print(f"Sent comment from {subreddit_name}: {comment_json}")
        except Exception as e:
            print("Error processing comment:", str(e))

def start_streaming():
    threads = []
    for subreddit in subreddits:
        thread = threading.Thread(target=stream_comments, args=(subreddit,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

#if __name__ == "__main__":
#    start_streaming()

start_streaming()
