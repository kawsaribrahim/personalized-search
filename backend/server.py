from elasticsearch import Elasticsearch
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import sys
import os

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "http://localhost:8080"}})

class Server:
    """ 
    A Server class to interact with the elastic search server
    """

    def __init__(self):
        try:
            from config import address, fingerprint, password
        except ImportError:
            print("Server Error: config.py not found")
            sys.exit(1)

        self.client = Elasticsearch(address, ssl_assert_fingerprint=fingerprint, basic_auth=("elastic", password))

    def create_index(self, index_name):
        if not self.client.indices.exists(index=index_name):
            self.client.indices.create(index=index_name)
            try:
                print("Indexing documents...")
                print(os.getcwd())
                
                count = 0
                for root, dirs, files in os.walk("../davisWiki"):
                    
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
                                self.client.index(index="document_index", id=count, document=doc)
                                if count % 5000 == 0:
                                    print(f"Processing document {count}")
                                
                            except Exception as e:
                                print(e)
                                print("title: ", title)
                                break

                print(f"Processing document {count}") 
                print("done")
            except Exception as e:
                print("Error while indexing")
                print(e)
            print(f"Index '{index_name}' created successfully.")
        else:
            print(f"Index '{index_name}' already exists.")

    def index_document(self, index_name, document):
        response = self.client.index(index=index_name, body=document)
        return response

    def search(self, index_name, query):
        response = self.client.search(index=index_name, body=query)
        return response


# App Implementation
es_server = Server()

@app.route('/api/index/create', methods=['POST'])
def create_index():
    data = request.get_json()
    index_name = data.get('index_name')
    es_server.create_index(index_name)
    return jsonify({'message': f"Index '{index_name}' created successfully."})

@app.route('/api/index/document', methods=['POST'])
def index_document():
    data = request.get_json()
    index_name = data.get('index_name')
    document = data.get('document')
    response = es_server.index_document(index_name, document)
    return jsonify(dict(response))

@app.route('/api/search', methods=['POST'])
def search():
    data = request.get_json()
    print("DATA: ", data)
    index_name = data.get('index_name')
    query = data.get('query')
    body={"query": {
        "match": {
            "text": query
    }}, "size": 30, "explain": True}
    response = es_server.search(index_name, body)
    return jsonify(dict(response))

if __name__ == '__main__':
    app.run(debug=True)