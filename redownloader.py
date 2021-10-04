import pandas as pd
import os

df = pd.read_csv("data/files.csv")

print(df)
zeros = exists = 0
for i,row in df.iterrows():
    if os.path.isfile(row["data"]):
        exists+=1
        if os.path.getsize(row["data"]) == 0:
           zeros+=1

print("Total: {}".format(df.shape[0]))
print("Downloaded: {}".format(exists))
print("Zeros: {}".format(zeros))
print("Non Zeros: {}".format(exists-zeros))
print("Percentage {}".format(100*zeros/exists))
