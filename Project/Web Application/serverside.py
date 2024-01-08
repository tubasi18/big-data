from flask import Flask, jsonify, render_template
from pymongo import MongoClient

app = Flask(__name__)

client = MongoClient('mongodb://localhost:27017')
db = client['bigdata']
tweets_collection = db['tweets']

def get_top_users():
    user_tweets= [
        {"$group": {"_id": "$user","count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20},
    ]
    result = list(tweets_collection.aggregate(user_tweets))
    return result

def get_tweet_distribution(user):
    user_tweets_date = [
        {"$match": {"user": user}},
        {"$group": {"_id": {"$dateToString": {"format": "%Y/%m/%d", "date": {"$dateFromString": {"dateString": "$date"}}}}, "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    result = list(tweets_collection.aggregate(user_tweets_date))
    return result

@app.route('/')
def index():
    return render_template('page.html')

@app.route('/top_users')
def top_users():
    top_users_data = get_top_users()
    return jsonify(top_users_data)

@app.route('/tweet_distribution/<user>')
def tweet_distribution(user):
    distribution_data = get_tweet_distribution(user)
    return jsonify(distribution_data)
if __name__ == '__main__':
    app.run(debug=True)
