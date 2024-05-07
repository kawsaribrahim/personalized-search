from elasticsearch import Elasticsearch
from ast import literal_eval
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
from itertools import chain
import time
import sqlite3
import sys
from pprint import pprint
import csv
maxInt = sys.maxsize
while True:
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "http://localhost:8080"}})

# Database to store click histories
history_db = sqlite3.connect("history.db", check_same_thread=False)
hist_cur = history_db.cursor()
if len(hist_cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='history'").fetchall()) == 0:
    hist_cur.execute("CREATE TABLE history (HISTORY_ID INTEGER PRIMARY KEY AUTOINCREMENT, USER_ID INTEGER, ARTICLE INTEGER, CATEGORIES TEXT)")
    history_db.commit()

# Database to store query histories
query_db = sqlite3.connect("queries.db", check_same_thread=False)
quer_cur = query_db.cursor()
if len(quer_cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='queries'").fetchall()) == 0:
    quer_cur.execute("CREATE TABLE queries (QUERY_ID INTEGER PRIMARY KEY AUTOINCREMENT, USER_ID INTEGER, QUERY TEXT)")
    query_db.commit()

print(quer_cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())

class Server:
    """ 
    A Server class to interact with the elastic search server
    """

    def __init__(self):

        try:
            from config import data_files, index, address, fingerprint, password
        except ImportError:
            print("Server Error: config.py not found")
            sys.exit(1)

        self.index_name = index
        self.client = Elasticsearch(address, ssl_assert_fingerprint=fingerprint, basic_auth=("elastic", password))
        info = self.client.info()
        print("Elasticsearch version:", info['version']['number'])
        
        if self.client.indices.exists(index=self.index_name):
            if (input("Do you want to delete the current index and create a new one? (y/n) ") in "yY"):
                self.client.indices.delete(index=self.index_name)
                self.create_fill(self.index_name, data_files)
        else:
            self.create_fill(self.index_name, data_files)
        
        if (len(hist_cur.execute("SELECT HISTORY_ID FROM history").fetchall()) != 0 or
           len(quer_cur.execute("SELECT QUERY_ID FROM queries").fetchall()) != 0):
            if (input("Do you want to delete the contents of the database? (y/n) ") in "yY"):
                hist_cur.execute("DELETE FROM history")
                history_db.commit()
                quer_cur.execute("DELETE FROM queries")
                query_db.commit()
                
            print("There are ", len(hist_cur.execute("SELECT HISTORY_ID FROM history").fetchall()), " entries in the history table.")
            print("There are ", len(quer_cur.execute("SELECT QUERY_ID FROM queries").fetchall()), " entries in the query table.")
    
    def create_fill(self, index_name, file_names):
        
        body = {
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "description": {"type": "text"},
                    "link": {"type": "text"},
                    "timestamp": {"type": "date"},
                    "category": {"type": "rank_features"},
                    "p1": {"type": "float"},
                    "p2": {"type": "float"},
                    "p3": {"type": "float"}
                }
            }
        }
        
        resp = self.client.indices.create(index=index_name, body=body)
        print("Index created successfully.")
        print(resp)
        self.fill_index(index_name, file_names)
        self.client.indices.refresh(index=index_name)
    
    def csv_loop(self, file_names):
        count = 0
        docs = {}
        BATCH = 800
        
        for file_name in file_names:
            with open(file_name, 'r', newline='', encoding="utf-8") as csvfile:
                try:
                    reader = csv.reader(csvfile)
                    flag = True
                    for row in reader:
                        # Skip the first row (header)
                        if flag:
                            flag = False
                            continue
                        title = row[0]
                        description = row[1]
                        link = row[2]
                        category = literal_eval(row[3])
                        doc = {
                            "title": title,
                            "description": description,
                            "link": link,
                            "category": {i: 1 for i in category},
                            "timestamp": datetime.now(),
                            "p1": "0.0",
                            "p2": "0.0",
                            "p3": "0.0"
                        }
                        count += 1
                        docs[count] = doc
                        if count % BATCH == 0:
                            yield docs
                            docs = {}
                        if count % 5000 == 0:
                            print(f"Processing document {count}")
                except Exception as e:
                    print(e)
                    print("title: ", title)
                
        yield docs
        print(f"Processing document {count}")
        print("done")

    def fill_index(self, index_name, file_names):
        try:
            print("Indexing documents...")

            for i in self.csv_loop(file_names):
                
                body = list(chain.from_iterable(({"index": {"_index": index_name, "_id":j}}, k) for (j,k) in i.items()))
                
                resp = self.client.bulk(body=body)
                    
        except Exception as e:
            print("Error while indexing")
            print(e)
        print(f"Index '{index_name}' created successfully.")

    def search(self, index_name, query, user_id):
        response = self.client.search(index=index_name, body=query)
        
        # log query
        self.log_query(user_id, query["query"]["bool"]["must"][0]["match"]["description"])
        
        return response
    
    def log_query(self, user_id, query):
        quer_cur = query_db.cursor()
        quer_cur.execute("INSERT INTO queries (USER_ID, QUERY) VALUES (?, ?)", (user_id, query))
        query_db.commit()
        
        print("There are ", len(quer_cur.execute("SELECT QUERY_ID FROM queries").fetchall()), " entries in the query table.")

    def log_click(self, user_id, result, categories):
        hist_cur = history_db.cursor()
        hist_cur.execute("INSERT INTO history (USER_ID, ARTICLE, CATEGORIES) VALUES (?, ?, ?)", (user_id, result, categories))
        history_db.commit()
        
        print("There are ", len(hist_cur.execute("SELECT HISTORY_ID FROM history").fetchall()), " entries in the history table.")
    


# App Implementation
es_server = Server()

@app.route('/api/search', methods=['POST'])
def search():
    data = request.get_json()
    print("Request: ", data)
    index_name = data.get('index_name')
    query = data.get('query')
    user_id = data.get('userID')
    print("User: ", user_id)
    user_scores = {}
    
    for article in hist_cur.execute("SELECT CATEGORIES FROM history WHERE USER_ID = " + str(user_id)).fetchall():
        for cat in literal_eval(article[0]):
            if cat in user_scores:
                user_scores[cat] += 1
            else:
                user_scores[cat] = 1
    
    body={"query": {
        "bool": {
            "must": [
                {"match": {"description": query}}
            ],
            "should": [
                {"rank_feature": {"field": "category."+ cat, "boost":score*5}} for (cat, score) in user_scores.items()
            ]
        }
        
    }, "size": 300, "explain": True}    
    
    response = es_server.search(index_name, body, user_id)

    return jsonify(dict(response))


@app.route('/api/click', methods=['POST'])
def log_click():
    data = request.get_json()
    print("Request: ", data)
    user_id = data.get('userID')
    print("User: ", user_id)
    
    print("Clicked detected:")
    pprint(data["result"]["_id"])
    
    es_server.log_click(user_id, data["result"]["_id"], repr(list(data["result"]["_source"]["category"].keys())))
    
    return jsonify({"status": "success"})
    
    

if __name__ == '__main__':
    app.run(debug=True)