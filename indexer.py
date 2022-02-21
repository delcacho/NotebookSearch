from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datasketch import MinHash, MinHashLSH
from pqdm.threads import pqdm
from nltk import ngrams
import datetime
from dbc_parser import parseDBC
from ipynb_parser import parseIPYNB
from config_parser import parseDatabricksCfg 
import glob
import pandas as pd
import tqdm
import sys
import traceback
import zipfile
import time

debugEnabled = False
indexName = "tmp-index-"+str(round(time.time()))
names, hosts, tokens = parseDatabricksCfg()

client = Elasticsearch("http://localhost:9200")

def findCanonicalDocument(df,docurls,url,oldindex):
   if url in docurls:
      return(docurls[url])
   return oldindex

def buildUrl(envname,objectid,location,envnames,hosts):
  for i in range(len(envnames)):
    if envnames[i] == envname:
       return hosts[i] + "#notebook/"+str(objectid)
  return envname + "/tree/main/" + location

def indexDocument(es,indexName,doc):
   res = es.index(index=indexName, body=doc)

def deleteIndex(es, indexName):
   es.indices.delete(index=indexName, ignore=[400, 404])

def renameIndex(es,indexName,alias):
   if client.indices.exists_alias(alias):
      srcIndex = list(client.indices.get_alias(alias).keys())[0]
      es.indices.update_aliases({
        "actions": [
           { "add":    { "index": indexName, "alias": alias }}, 
           { "remove": { "index": srcIndex, "alias": alias  }} 
        ]
      })
      deleteIndex(es,srcIndex)
   else:
      es.indices.put_alias(index=indexName,name=alias)

def createIndex(es, indexName):
   mapping = {
       "mappings": {
   #        "dynamic": "runtime",
           "properties": { 
               "title": { "type": "text", "analyzer": "standard" },
               "body": { "type": "text"},
               "language": { "type": "keyword" },
               "author": { "type": "text" },
               "tags": {"type": "keyword"},
               "url": {"type": "keyword"},
               "envname": { "type": "keyword" },
               "vertical": { "type": "keyword" },
               "step": { "type": "keyword" },
               "timestamp": {"type": "date"},
               "lastRun": {"type": "date"}
           }
       }
   }
   print(es.indices.create(index=indexName,body=mapping))

def parseDocument(i,row):
  global df
  try:
     location = row["location"]
     if location.startswith("/Users/"):
        user = location[7:]
        user = user[0:user.find("/")]
     else:
        user = "Unknown"
     if row["data"].endswith(".dbc"):
        doc = parseDBC(row["data"])
     elif row["data"].endswith(".ipynb"):
        doc = parseIPYNB(row["data"])
     else:
        raise NotImplementedError
     doc["author"] = user
     doc["envname"] = row["envname"]
     doc["timestamp"] = datetime.datetime.now()
     doc["_index"] = indexName
     doc["url"] = buildUrl(row["envname"],row["objectid"],row["location"],names,hosts)
     return doc
  except:
     return {}
     pass

def hashRecord(i,row):
  global lsh, docstore
  if bool(docstore[i]):
     minhash = MinHash(num_perm=256)
     for d in docstore[i]["body"].split():
        minhash.update("".join(d).encode('utf-8'))
     return minhash

deleteIndex(client,indexName)
createIndex(client,indexName)

df = pd.read_csv("data/files.csv")
if debugEnabled:
    df = df.tail(500)
    df.reset_index(drop=True,inplace=True)
df.sort_values('objectid')

print("Going to Parse DBCs!")
docstore = pqdm(df.iterrows(), parseDocument, n_jobs=2, argument_type='args')
docurls = {(doc["url"] if "url" in doc else ""):i for (i,doc) in enumerate(docstore)}

for i,row in df.iterrows():
  doc = docstore[i]
  if "canonicalUrl" in doc:
     rowno = findCanonicalDocument(df,docurls,doc["canonicalUrl"],i)
     df.loc[rowno,"objetcid"] = 0
     docstore[rowno]["url"] = doc["canonicalUrl"]
     if "canonicalAuthor" in doc:
        docstore[rowno]["author"] = doc["canonicalAuthor"]

reordered = df["objectid"].sort_values().index
df.sort_values('objectid')
docstore = [docstore[i] for i in reordered]

print("Going to MinHash!")
lsh = MinHashLSH(threshold=0.9, num_perm=256)

minhashes = pqdm(df.iterrows(), hashRecord, n_jobs=2, argument_type='args')
print("Going to index!")
docs = []
for i,row in tqdm.tqdm(df.iterrows()):
  if minhashes[i] is not None:
     result = lsh.query(minhashes[i])
     lsh.insert(i, minhashes[i])
     if len(result) == 0:
        if docstore[i] is not None:
           docs.append(docstore[i])
        if i%1000 == 0 and i > 0:
           res = helpers.bulk(client, docs, chunk_size=1000, request_timeout=200)
           docs = []
if len(docs) > 0:
  res = helpers.bulk(client, docs, chunk_size=1000, request_timeout=200)

renameIndex(client,indexName,"notebookIndex")
