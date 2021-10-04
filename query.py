from elasticsearch import Elasticsearch
from elasticsearch import helpers
import datetime
from dbc_parser import parseDBC
import glob
import pandas as pd
import tqdm

# Instantiate a client instance
es = Elasticsearch("http://localhost:9200")

while True:
  token = input('Query> ').lower()
  if token == "quit":
    break
  query = {
    "query": {
      "bool" : {
        "should" : {
          "term" : { "title" : token },
          "term" : { "body" : token }
        }
      }
    }
  }

  print(es.search(index="notebook-index",body=query))
