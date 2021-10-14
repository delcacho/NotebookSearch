import sys
import csv
import os
import traceback
from subprocess import PIPE, run
from joblib import Parallel, delayed

outcsv = open("./data/files.csv","a")
writer = csv.writer(outcsv)
#(["envname","objectid","location","data"])

def out(command):
    result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    return result.stdout

with open("repos.ini","r") as fp:
  lines = fp.readlines()
lines = list(set([line.strip() for line in lines]))

def process(line):
   repo = line.strip()
   print("Processing",repo,"...")
   output = out("svn ls -R {}.git".format(repo))
   folder = "./data/git"+line[repo.rfind("/",0,repo.rfind("/")):]
   try:
     os.makedirs(folder)
   except:
     pass
   for outline in output.splitlines():
     outline = outline.strip()
     if outline.endswith(".dbc") or outline.endswith(".ipynb"):
        objectid = outline.replace("trunk/","")
        writer.writerow([repo,0,objectid,folder+outline[outline.rfind("/"):]])
        out("svn export {}.git/{} {}".format(line,outline,folder))
try:
  os.mkdir("./data/git")
except:
  pass

for line in lines:
   process(line)

outcsv.close()
