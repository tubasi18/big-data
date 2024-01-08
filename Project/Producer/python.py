import csv
import re
import time
from confluent_kafka import Producer
#Using a locally configured Kafka producer, 
#this Python function sends a list of tweets to the 'twitter' topic.
def produce_to_kafka(tweet_list):
    producer_conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(producer_conf)
    for tweet in tweet_list:
        producer.produce('twitter', value=str(tweet))
    producer.flush()
#This code reads the first 100 rows from a CSV file, 
#extracts specific fields, structures them into a dictionary format,
#sends batches of 100 tweets to a Kafka topic using the function produce_to_kafka, 
#and pauses for 2 seconds between batches.
i = 1
tweets = []
with open('archive/training.csv', 'r') as isfile:
    reader = csv.reader(isfile)
    for row in reader:
        if i <= 100 :
            tweet = {
            "id": row[1],
            "date": row[2],
            "user": row[4],
            "text": re.sub(r"'text': '|'", '', row[5]),
            "producedTweetTime": ""
            }
            tweets.append(tweet)        
        else:
            i = 1
            produce_to_kafka(tweets)
            tweets = [] 
            time.sleep(2)
        i += 1