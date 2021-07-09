import os
from flask import Flask, jsonify
import json

app = Flask(__name__)


@app.route('/')
def return_data():
    """Web service to display previously scraped data.

    :param: file (data.json) - must be actually in the same directory as this file.
    """

    data = []
    filename = 'data.json'
    with open(os.path.join(os.getcwd(), filename), 'r') as f:  # open in readonly mode
             data.append([json.loads(line) for line in f])
    counts = len(data[0][0]['products'])
    print(counts)
    return jsonify(counts, data)
