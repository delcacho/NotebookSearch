from zipfile import ZipFile
import json
import pandas as pd
import ahocorasick
import numpy as np
from memoization import cached

def usesPackage(automaton,package,tag):
    automaton.add_word("import "+package,tag)
    automaton.add_word("from "+package,tag)
    automaton.add_word("require("+package+")",tag)
    automaton.add_word("library("+package+")",tag)

def findTags(aut,element):
    result = set()
    try:
        for k,vlist in aut.iter(element):
          for v in vlist:
             result.add(v)
    except:
        pass
    return list(result)

@cached
def getFilters(fname):
   filters = pd.read_csv(fname)
   grouped = filters.groupby('expression')
   filters = grouped.aggregate(lambda x: list(x))
   filters.reset_index(inplace=True)
   automaton = ahocorasick.Automaton()
   for i,row in filters.iterrows():
      if row["filter_type"][0] == "EXACT":
         automaton.add_word(row["expression"],row["tag"])
      else:
         usesPackage(automaton,row["expression"],row["tag"])
   automaton.make_automaton()
   return automaton

def extractZip(input_zip):
    input_zip=ZipFile(input_zip)
    return {name: input_zip.read(name) for name in input_zip.namelist()}
