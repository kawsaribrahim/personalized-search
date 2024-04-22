''' Usage: Create a python file in public/ with the name "config.py" and add the following code variables:
    Configure the variables according to your Elasticsearch instance.

# Initialization of API keys

# Elasticsearch address:
address = "https://localhost:9200"

# Password for the elastic user (reset with `bin/elasticsearch-reset-password -u elastic`):
password = "7l5NNDht_jsMzzExtWUo"

# HTTP CA certificate SHA-256 fingerprint:
fingerprint = "0fb63522175c33c1842b5b5678986c10fa78b04ae264032beda52c2578683a8e"

# Enrollment token:
enrollment = "eyJ2ZXIiOiI4LjEzLjIiLCJhZHIiOlsiMTcyLjE4LjAuMzo5MjAwIl0sImZnciI6IjdhYzJmN2U2MGRmOWE0ZTMwMGQwMDAyODc1MWNiMjhjZWVmOTQyMmE2ZGM3OWQ4MWRmZjMwNWEyY2FkNTcyNjEiLCJrZXkiOiJsWjhjNjQ0QklqQkg5UVVZREQ4NDpURndmNng0OVFHU1dCeHpMd1FXNGVRIn0="
  
'''
from elasticsearch import Elasticsearch
from datetime import datetime
import sys
import os

try:
    from config import *
except ImportError:
    print("Error: config.py not found")
    sys.exit(1)

client = Elasticsearch(address, ssl_assert_fingerprint=fingerprint, basic_auth=("elastic", password))
print(client.info())

try:
    print("Indexing documents...")
    client.indices.create(index="document_index")
    
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
                    client.index(index="document_index", id=count, document=doc)
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
    if (input("Do you want to delete the index? (y/n) ") in "yY"):
        client.indices.delete(index="document_index")
    
try:
    query = "zombie attack"

    resp = client.search(index="document_index", body={"query": {
        "match": {
            "text": query
    }}, "size": 10, "explain": True})

    count = 0 
    print(f"Got {resp['hits']['total']['value']} hits for the query {query}. Hits:")
    for hit in resp['hits']['hits']:
        print("%(title)s" % hit["_source"])
        
except Exception as e:
    print("Error while searching")
    print(e)

