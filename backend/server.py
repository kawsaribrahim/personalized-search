from elasticsearch import Elasticsearch
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import sys
import csv

app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "http://localhost:8080"}})

class Server:
    """ 
    A Server class to interact with the elastic search server
    """

    def __init__(self):

        try:
            from config import data_file, index, address, fingerprint, password
        except ImportError:
            print("Server Error: config.py not found")
            sys.exit(1)

        self.index_name = index
        self.client = Elasticsearch(address, ssl_assert_fingerprint=fingerprint, basic_auth=("elastic", password))
        
        if self.client.indices.exists(index=self.index_name):
            if (input("Do you want to delete the current index and create a new one? (y/n) ") in "yY"):
                self.client.indices.delete(index=self.index_name)
                self.client.indices.create(index=self.index_name)
                self.fill_index(self.index_name, data_file)
        else:
            self.client.indices.create(index=self.index_name)
            self.fill_index(self.index_name, data_file)

    def fill_index(self, index_name, file_name):
        try:
            print("Indexing documents...")

            count = 0
            with open(file_name, 'r', newline='', encoding="utf-8") as csvfile:
                try:
                    reader = csv.reader(csvfile)
                    for row in reader:
                        title = row[0]
                        description = row[1]
                        link = row[2]
                        doc = {
                            "title": title,
                            "description": description,
                            "link": link,
                            "timestamp": datetime.now()
                        }
                        count += 1
                        self.client.index(index=index_name, id=count, document=doc)
                        if count % 5000 == 0:
                            print(f"Processing document {count}")

                except Exception as e:
                    print(e)
                    print("title: ", title)
                    
            print(f"Processing document {count}")
            print("done")
                    
        except Exception as e:
            print("Error while indexing")
            print(e)
        print(f"Index '{index_name}' created successfully.")

    def search(self, index_name, query):
        response = self.client.search(index=index_name, body=query)
        return response


# App Implementation
es_server = Server()

@app.route('/api/search', methods=['POST'])
def search():
    data = request.get_json()
    print("Request: ", data)
    index_name = data.get('index_name')
    query = data.get('query')
    body = {
        "query": {
            "query_string": {
                "query": query
            }
        },
        "size": 10000
    }
    response = es_server.search(index_name, body)
    return jsonify(dict(response))

if __name__ == '__main__':
    app.run(debug=True)