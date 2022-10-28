import glob
import csv
import random
from datetime import datetime
import langdetect
import re
import os

print("Currente dir: ",os.getcwd())

def get_jaccard_sim(str1, str2):
    a = set(str1.split())
    b = set(str2.split())
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))


########################################################
#parameters
start_date=datetime(2018,1,1)
end_date=datetime(2023,1,1)
sample_size=500
similarity_threshold=0.7 #between 0 and 1
#language_threshold=0.7 #between 0 and 1
########################################################

tweets = {}
sample = []

def detectDelimiter(csvFile):
    with open(csvFile, 'r',encoding="utf-8") as myCsvfile:
        header=myCsvfile.readline()
        if header.find(";")!=-1:
            return ";"
        if header.find(",")!=-1:
            return ","
    #default delimiter (MS Office export)
    return ","


#read all the tweets from all the files
num_files=0
for file_name in glob.glob("2. conversazioni da quotidiani.csv"):

    delimiter=detectDelimiter(file_name)

    num_files+=1
    file=open(file_name,encoding="utf-8")
    csv_file=csv.reader(file, delimiter=delimiter,quotechar="\"")
    first=1
    for row in csv_file:
        if len(row)==0:
            continue
        if first:
            first=0
            continue
        tweets[row[9]]=row+[file_name.split("/")[-1]]
    file.close()

#filter the tweets
print("recoveder ",len(tweets.keys())," distinct tweets from ",num_files, "files")
tweets=[*tweets.values()]
random.shuffle(tweets)
for tweet in tweets:
    print(tweet)
    if len(sample)>=sample_size:
        continue
    #filter by date
    #date= datetime. strptime(tweet[2], '%Y-%m-%dT%H:%M:%S.000Z')
    #if not (date >= start_date and date <= end_date):
    #    continue

    #filter by source
    print("source ",tweet[13])
    if tweet[13] not in ['Twitter for iPad',
                      'Twitter Web App',
                      'Twitter for iPhone',
                      'Twitter Web Client',
                      'Twitter',
                      'Twitter for Mac',
                      'Twitter for Android']:
        continue

    #filter by text token lenght
    print("text ",tweet[12])
    if len(tweet[8].split(" "))<5 or len(tweet[12].split(" "))<5:
            continue

    #filter by language
    prediction=langdetect.detect(tweet[12].replace("\n"," "))
    if prediction != 'it':
        continue


    #filter if text contains an url
    regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    url = re.findall(regex,tweet[12])
    if len(url)>0:
        continue

    #filter by similarity
    flag=False
    for sampled_tweet in sample:
        if flag:
            continue
        similarity=get_jaccard_sim(tweet[12],sampled_tweet[12])
        if round(similarity,1)>=similarity_threshold:
            flag=True
    if flag:
        continue

    #OPZIONALE: pensa ad un filtro che potresti applicare utilizzando retweet_count, reply_count, like_count, quote_count

    sample.append(tweet)

print("final sample of size: ",len(sample))
print("saving sample in a file")
output=open("3. campione finale.csv","w",encoding="utf-8")
output_csv=csv.writer(output,delimiter=";",quotechar="\"",quoting=csv.QUOTE_ALL)
for tweet in sample:
    output_csv.writerow(tweet)
output.close()
print("sample saved!")
