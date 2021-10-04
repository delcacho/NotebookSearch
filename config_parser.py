import os

def parseDatabricksCfg():
   names = []
   hosts = []
   tokens = []
   with open(os.path.expanduser('~/.databrickscfg'),'r') as fp:
      lines = fp.readlines()
      for line in lines:
        if line.startswith("["):
           name = line[1:]
           name = name.replace("]","").strip()
           names.append(name)
        if line.startswith("host"):
           host = line[line.find("=")+1:].strip()
           hosts.append(host)
        if line.startswith("token"):
           token = line[line.find("=")+1:].strip()
           tokens.append(token)
   return(names,hosts,tokens)
