from elasticsearch import Elasticsearch
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
from itertools import chain
import time
import sys
from pprint import pprint
import os

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "http://localhost:8080"}})

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
    
    def create_fill(self, index_name, file_name):

        body = {
            "settings": {
                "similarity": {
                    "personalized_similarity": {
                        "type": "scripted",
                        "script": { 
                            "source": """double tf = Math.sqrt(doc.freq); 
                                    double idf = Math.log((field.docCount+1.0)/(term.docFreq+1.0)) + 1.0; 
                                    double norm = 1/Math.sqrt(doc.length); 
                                    double boost = 2.2;
                                    return boost * tf * idf * norm;""",
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "text": {
                        "type": "text",
                        "similarity": "personalized_similarity",
                        "fields": {
                            "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                            }
                        }
                    }
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
                            "timestamp": datetime.now()
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

        return response
    


# App Implementation
es_server = Server("document_index", "../davisWiki")

@app.route('/api/search', methods=['POST'])
def search():
    data = request.get_json()
    print("DATA: ", data)
    index_name = data.get('index_name')
    query = data.get('query')
    
    body={"query": {
        "match": {
            "text": query,
    }}, "size": 30, "explain": True}
    
    response = es_server.search(index_name, body)
    pprint([ i["_source"]["title"] for i in response["hits"]["hits"]])
    pprint([ i["_score"] for i in response["hits"]["hits"]])
    pprint(response["hits"]["hits"][0])


    return jsonify(dict(response))

if __name__ == '__main__':
    app.run(debug=True)