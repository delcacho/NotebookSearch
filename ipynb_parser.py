from zipfile import ZipFile
import json
import pandas as pd
import ahocorasick
import numpy as np
from memoization import cached
from common_parser import usesPackage, findTags, getFilters, extractZip

def readMetadata(txt,doc):
   offset = 0
   pos = txt.find("<!--",offset)
   while pos >= 0:
     offset = txt.rfind("-->",pos)
     comment = txt[pos+len("<-!--"):offset+1-len("-->")]
     comment = comment.strip()
     if comment.startswith("[metadata"):
       bracketstart = comment.find("{")
       bracketend = comment.rfind("}")
       obj = json.loads(comment[bracketstart:bracketend+1])
       for vals in obj["search_tags"].items():
          doc[vals[0]] = vals[1]
     pos = txt.find("<!--",offset)

def parseIPYNB(ipynb_file):
    txt = ''
    if ipynb_file.endswith(".zip"):
       files = extractZip(ipynb_file)
       ipynb_file = ipynb_file.replace(".ipynb.zip",".ipynb")
       name,contents = list(files.items())[0]
    else:
       with open(ipynb_file,"r") as fp: 
          contents = fp.read()
    obj = json.loads(contents)
    try:
      language = obj["metadata"]["language_info"]["name"].lower()
    except:
      language = list(obj["metadata"].values())[0]["language"].lower()
      pass
    if language == "sql":
      language = language.upper()
    else:
      language = language.capitalize()
    name = ipynb_file[ipynb_file.rfind("/")+1:].replace(".ipynb","")
    for command in obj["cells"]:
      if command["cell_type"] == "code" or command["cell_type"] == "markdown":
         txt += "".join(command["source"])+"\n"
    tags = findTags(getFilters("./filters.csv"),txt)
    doc = {"language": language, "title": name, "body": txt, "tags": tags}
    readMetadata(txt,doc)
    return(doc)

if __name__ == "__main__":
    print(parseIPYNB("demo.ipynb.zip"))
