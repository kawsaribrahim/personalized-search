from elasticsearch import Elasticsearch

from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
from itertools import chain
import time
import sqlite3
import sys
from pprint import pprint
import os

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "http://localhost:8080"}})

# Database to store click histories
history_db = sqlite3.connect("history.db", check_same_thread=False)
hist_cur = history_db.cursor()
if len(hist_cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='history'").fetchall()) == 0:
    hist_cur.execute("CREATE TABLE history (HISTORY_ID INTEGER PRIMARY KEY AUTOINCREMENT, USER_ID INTEGER, ARTICLE INTEGER)")
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

    def __init__(self, index_name, file_name):
        self.index_name = index_name

        try:
            from config import address, fingerprint, password
        except ImportError:
            print("Server Error: config.py not found")
            sys.exit(1)

        self.client = Elasticsearch(address, ssl_assert_fingerprint=fingerprint, basic_auth=("elastic", password))
        
        if self.client.indices.exists(index=index_name):
            if (input("Do you want to delete the contents of the index? (y/n) ") in "yY"):
                self.client.indices.delete(index=self.index_name)
                self.create_fill(self.index_name, file_name)
        else:
            self.create_fill(self.index_name, file_name)
        
        if (len(hist_cur.execute("SELECT HISTORY_ID FROM history").fetchall()) != 0 or
           len(quer_cur.execute("SELECT QUERY_ID FROM queries").fetchall()) != 0):
            if (input("Do you want to delete the contents of the database? (y/n) ") in "yY"):
                hist_cur.execute("DELETE FROM history")
                history_db.commit()
                quer_cur.execute("DELETE FROM queries")
                query_db.commit()
                
            print("There are ", len(hist_cur.execute("SELECT HISTORY_ID FROM history").fetchall()), " entries in the history table.")
            print("There are ", len(quer_cur.execute("SELECT QUERY_ID FROM queries").fetchall()), " entries in the query table.")
    
    def create_fill(self, index_name, file_name):
        
        body = {
            "mappings": {
                "properties": {
                    "title": {"type": "text"},
                    "text": {"type": "text"},
                    "timestamp": {"type": "date"},
                    "p1": {"type": "float"},
                    "p2": {"type": "float"},
                    "p3": {"type": "float"}
                }
            }
        }
        
        resp = self.client.indices.create(index=index_name, body=body)
        print("Index created successfully.")
        print(resp)
        self.fill_index(index_name, file_name)
        self.client.indices.refresh(index=index_name)
    
    def file_loop(self, file_name):
        count = 0
        docs = {}
        BATCH = 800
        
        for root, dirs, files in os.walk(file_name):
            for file in files:
                with open(os.path.join(root, file), "r", encoding="utf-8") as f:
                    try:
                        title = file
                        text = f.read()
                        doc = {
                            "title": title,
                            "text": text,
                            "timestamp": datetime.now(),
                            "p1": 0,
                            "p2": 0,
                            "p3": 0
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
                        break
            yield docs
            print(f"Processing document {count}")

    def fill_index(self, index_name, file_name):
        try:
            print("Indexing documents...")
            print(os.getcwd())

            start = time.time()
            for i in self.file_loop(file_name):
                
                body = list(chain.from_iterable(({"index": {"_index": index_name, "_id":j}}, k) for (j,k) in i.items()))
                
                resp = self.client.bulk(body=body)
            
            print("done!")
            print("Indexing took: ", time.time() - start, "s")
            
        except Exception as e:
            print("Error while indexing")
            print(e)
        print(f"Index '{index_name}' created successfully.")

    def search(self, index_name, query):
        response = self.client.search(index=index_name, body=query)
        
        print(query["query"]["script_score"]["query"]["match"]["text"])
        # log query
        self.log_query(1, query["query"]["script_score"]["query"]["match"]["text"])

        return response
    
    def log_query(self, user, query):
        quer_cur = query_db.cursor()
        quer_cur.execute("INSERT INTO queries (USER_ID, QUERY) VALUES (?, ?)", (user, query))
        query_db.commit()
        
        print("There are ", len(quer_cur.execute("SELECT QUERY_ID FROM queries").fetchall()), " entries in the query table.")

    def log_click(self, user, result):
        hist_cur = history_db.cursor()
        hist_cur.execute("INSERT INTO history (USER_ID, ARTICLE) VALUES (?, ?)", (user, result))
        history_db.commit()
        
        print("There are ", len(hist_cur.execute("SELECT HISTORY_ID FROM history").fetchall()), " entries in the history table.")
    


# App Implementation
es_server = Server("document_index", "../davisWiki")

@app.route('/api/search', methods=['POST'])
def search():
    data = request.get_json()
    print("DATA: ", data)
    index_name = data.get('index_name')
    query = data.get('query')
    
    body={"query": {
        "script_score": {
            "query": {
                "match": {
                    "text": query
                }
            },
            "script": {
                "source": "_score + 1 * doc['p1'].value + 0 * doc['p2'].value + 0 * doc['p3'].value"
            }
        }
    }, "size": 100, "explain": True}    
    
    response = es_server.search(index_name, body)


    return jsonify(dict(response))


@app.route('/api/click', methods=['POST'])
def log_click():
    # To-Do: Current assumption is user 1 is logging
    data = request.get_json()
    print("Clicked detected:")
    pprint(data["result"]["_id"])
    
    es_server.log_click(1, data["result"]["_id"])
    
    return jsonify({"status": "success"})
    
    

if __name__ == '__main__':
    app.run(debug=True)