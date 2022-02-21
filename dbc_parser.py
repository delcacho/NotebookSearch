from zipfile import ZipFile
import json
import pandas as pd
import ahocorasick
import numpy as np
from memoization import cached
from common_parser import usesPackage, findTags, getFilters, extractZip
from datetime import datetime

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

def parseDBC(dbc_file):
    txt = ''
    files = extractZip(dbc_file)
    minTime = 9999999999999
    for name,contents in files.items():
      obj = json.loads(contents.decode("utf-8"))
      language = obj["language"]
      if language == "sql":
        language = language.upper()
      else:
        language = language.capitalize()
      name = obj["name"]
      for command in obj["commands"]:
        txt += command["command"]+"\n"
        if command["startTime"] < minTime and command["startTime"] > 0:
           minTime = command["startTime"]
    tags = findTags(getFilters("./filters.csv"),txt)
    minTime = datetime.fromtimestamp(minTime/1000.0)
    doc = {"language": language, "title": name, "body": txt, "tags": tags, "lastRun": minTime}
    readMetadata(txt,doc)
    return(doc)

if __name__ == "__main__":
    print(parseDBC("quentin.dbc"))
