from databricksapi.DBFS import DBFS
from databricksapi.Jobs import Jobs
from databricksapi.Clusters import Clusters
from databricksapi.Workspace import Workspace
from config_parser import parseDatabricksCfg
from concurrent.futures import ThreadPoolExecutor
import base64
import os
import pandas as pd
import traceback
import sys
import csv
import time

names, hosts, tokens = parseDatabricksCfg()
outcsv = open("./data/files.csv","w")
writer = csv.writer(outcsv)
writer.writerow(["envname","objectid","location","data"])
outcsv.flush()
executor = ThreadPoolExecutor(max_workers=5)

def getFileName(remotePath,envname):
   filename = remotePath[1+remotePath.rfind("/"):]
   if os.path.isfile("./data/"+envname+"/"+filename+".dbc"):
      i = 1
      while os.path.isfile("./data/"+name+"/"+filename+"_"+str(i)+".dbc"):
         i += 1
      filename = filename+"_"+str(i)
   filename = "./data/"+envname+"/"+filename+".dbc"
   fp = open(filename,"w")
   fp.write("")
   fp.close()
   return filename

def downloadNotebook(wksp,fpath,envname,filename):
   for i in range(10):
      try:
         data = wksp.exportWorkspace(fpath,"DBC","true")
         break
      except:
         traceback.print_exc()
         time.sleep(30)
         pass
   fp = open(filename,"wb")
   fp.write(data)
   fp.close()

def listDBFS(remotePath,url,token):
   dbfs = DBFS(url=url,token=token)
   result = dbfs.listFiles(remotePath)
   if "files" in result:
     for file in result["files"]:
      if ".dbc" in file["path"]:
         print(file["path"])
      if file["is_dir"]:
        listDBFS(file["path"])

def listWorkspace(remotePath,url,token,envname):
 try:
   if not os.path.isdir("./data/"+envname):
      os.mkdir("./data/"+envname)
   wksp = Workspace(url=url,token=token)
   for i in range(10):
      try:
         result = wksp.listWorkspace(remotePath)
         break
      except:
         traceback.print_exc()
         time.sleep(30)
         pass

   if "objects" in result:
     for file in result["objects"]:
      if ".dbc" in file["path"]:
         print(file["path"])
      if file["object_type"] == "DIRECTORY":
        listWorkspace(file["path"],url,token,envname)
      if file["object_type"] == "NOTEBOOK":
        fileName = getFileName(file["path"],envname)
        executor.submit(downloadNotebook,wksp,file["path"],envname,fileName)
        #downloadNotebook(wksp,file["path"],envname,fileName)
        writer.writerow([envname,str(file["object_id"]),file["path"],fileName])
        outcsv.flush()
 except Exception as e:
  traceback.print_exc()
  pass

for i in range(len(hosts)):
   url = hosts[i]
   token = tokens[i]
   name = names[i]
   print(name,url,token)
   listWorkspace("/",url,token,name)

executor.shutdown(wait=True)
outcsv.close()
