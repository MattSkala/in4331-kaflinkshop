from flask import Flask, request
from kafka import KafkaConsumer, KafkaProducer
import json

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC_USERS = 'users'

@app.route('/')
def hello():
    return 'Welcome to KaflinkShop!'

@app.route('/users/create/', methods=['POST'])
def create_user():
    username = request.form['username']
    producer.send(TOPIC_USERS, bytes('create_user ' + username))
    # TODO: wait for response
    return json.dumps({
        'user_id': 0
    })

@app.route("/users/remove/<user_id>", methods=["DELETE"])
def remove_user(user_id):    
    producer.send(TOPIC_USERS, bytes('remove_user ' + user_id))
    # TODO: wait for response
    return json.dumps({
        "success": False
    })

@app.route("/users/find/<user_id>", methods=["GET"])
def find_user(user_id):
    producer.send(TOPIC_USERS, bytes('find_user ' + user_id))
    # TODO: wait for response
    return json.dumps({
        "user_id": user_id
    })
