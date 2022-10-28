import requests #istallare
import json
import time
from datetime import datetime
import csv


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers, params):

    response = requests.request("GET", url, headers=headers, params=params)
    #print(response.status_code)
    #print(response.headers)
    if response.status_code==503:
        print("wait for ",30," seconds")
        time.sleep(30)
        return response.status_code, {}

    if int(response.headers['x-rate-limit-remaining'])>1:
        print(response.headers['x-rate-limit-limit'],response.headers['x-rate-limit-limit'])
        if response.headers['x-rate-limit-limit']=='3000':
            response.headers['x-rate-limit-limit']='300'
        print("wait for ",(15*60/int(response.headers['x-rate-limit-limit']))," seconds")
        time.sleep((15*60/int(response.headers['x-rate-limit-limit'])))
    else:
        print("wait for ",int(response.headers['x-rate-limit-reset'])-time.time()," seconds")
        time.sleep(int(response.headers['x-rate-limit-reset'])-time.time())

    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.status_code,response.json()
conversation_ids=[
]
file=open("1. tweet di quotidiani.csv")
spamreader=csv.reader(file,delimiter=",",quotechar="\"")
first=True
for row in spamreader:
    if first:
        first=False
        continue
    id=row[0].replace("_","")
    conversation_ids.append(id)
    
#da decommmentare in fase di test
#conversation_ids=conversation_ids[:10]

print("Tweet da recuperare: ", len(conversation_ids))

search_url = "https://api.twitter.com/2/tweets/search/all"
bearer_token='AAAAAAAAAAAAAAAAAAAAAARzMQEAAAAABg5mjk7Sr0sSq8Gpyao9kboKhuE%3D0cNePyaHNsGyrUiYiacnGmDKMALOZCD7bJV8o41ReXJVyyblzx'
headers = create_headers(bearer_token)

file_name= '2. conversazioni da quotidiani.csv'
file = open(file_name, "w",encoding="utf-8")
csv_file=csv.writer(file,delimiter=",",quotechar="\"")

csv_file.writerow([
           'conversation_id','conversation_author_id','conversation_author_username', 'conversation_created_at','conversation_text',
           'replied_to_id','replied_to_author_id','replied_to_created_at','replied_to_text',
           'id','author_id','created_at','text',
           'source',
           'retweet_count','reply_count','like_count','quote_count','url','domain',
           #'json'
                    ]
                  )
for conversation_id in conversation_ids:

    #recover info about conversation_id
    conversation_author_id=None
    conversation_created_at=None
    conversation_text=None
    tweet_fields = "tweet.fields=lang,author_id,created_at"
    ids = "ids="+str(conversation_id)
    url = "https://api.twitter.com/2/tweets?{}&{}".format(ids, tweet_fields)
    response = requests.request("GET", url, headers=headers)

    status = response.status_code
    json_response = response.json()

    if status==200:
        for jsonTweet in json_response['data']:
            if str(jsonTweet['id'])==str(conversation_id):
                conversation_author_id=jsonTweet['author_id']
                conversation_created_at=jsonTweet['created_at']
                conversation_text=jsonTweet['text']
    time.sleep(5)

    next_token=None
    count=0
    while next_token!=0:
        query_params = {
                'query': 'conversation_id : '+str(conversation_id) #tutta la conversazione
                #'query': 'in_reply_to_status_id : '+str(conversation_id) #solo le risposte dirette
            ,
            'max_results': 500,
            'next_token' : next_token,
            'start_time':datetime.strftime(datetime(2007,1,1,0,0,0), '%Y-%m-%dT%H:%M:%SZ'),
            #'end_time':  datetime.strftime(datetime(2022,5,26,10,0,0),    '%Y-%m-%dT%H:%M:%SZ'),
            'expansions':
                         'author_id,'
                         'geo.place_id,'
                         'in_reply_to_user_id,'
                         'referenced_tweets.id,'
                         'referenced_tweets.id.author_id',
            'tweet.fields':'author_id,'
                           'created_at,'
                           'public_metrics,'
                           'source,'
                           'conversation_id,'
                           'entities',
                            }
        status=0
        while status!=200:
            try:
                status,json_response = connect_to_endpoint(search_url, headers, query_params)
                #print(status)
            except Exception as e:
                print(e)
                print(status)
                time.sleep(30)

        #print(json.dumps(json_response['meta']))
        if 'meta' in json_response:
            if 'next_token' in json_response['meta']:
                next_token=json_response['meta']['next_token']
            else:
                next_token=0
        else:
            next_token=0

        if 'data' in json_response:
            print(len(json_response['data'])," tweets recovered")
            print(json.dumps(json_response['data']))
            print(json.dumps(json_response['includes']))
            for tweet in json_response['data']:
                count+=1

                file = open(file_name, "a",encoding="utf-8")
                csv_file=csv.writer(file,delimiter=",",quotechar="\"")

                #recover urls and domains
                urls=''
                domains=''
                if 'entities' in tweet:
                    if 'urls' in tweet['entities']:
                        #print(tweet)
                        urls=','.join([t['url'] for t in tweet['entities']['urls']])
                        domains=','.join([t['expanded_url'].split("/")[2] for t in tweet['entities']['urls']])

                #recover info about replied_to_id
                replied_to=None
                for referenced_tweet in tweet['referenced_tweets']:
                    print(referenced_tweet['type'])
                    if referenced_tweet['type']=='replied_to':
                        replied_to=referenced_tweet['id']

                replied_to_id=None
                replied_to_author_id=None
                replied_to_created_at=None
                replied_to_text=None
                if'tweets' in json_response['includes']:
                    for included_tweet in json_response['includes']['tweets']:
                        if included_tweet['id']==replied_to:
                            replied_to_id=included_tweet['id']
                            replied_to_author_id=included_tweet['author_id']
                            replied_to_created_at=included_tweet['created_at']
                            replied_to_text=included_tweet['text']

                conversation_author_username=None
                if 'users' in json_response['includes']:
                    for referenced_user in json_response['includes']['users']:
                            if str(referenced_user['id'])==str(conversation_author_id):
                                #print(referenced_user)
                                conversation_author_username=referenced_user['username']

                csv_file.writerow([
                                   "_"+str(conversation_id),
                                   "_"+str(conversation_author_id),
                                   "_"+str(conversation_author_username),
                                   conversation_created_at,
                                   conversation_text,
                                   "_"+str(replied_to_id),
                                   "_"+str(replied_to_author_id),
                                   replied_to_created_at,
                                   replied_to_text,
                                   "_"+str(tweet['id']),
                                   "_"+str(tweet['author_id']),
                                   tweet['created_at'],
                                   tweet['text'],
                                   tweet['source'],
                                   tweet['public_metrics']['retweet_count'],
                                   tweet['public_metrics']['reply_count'],
                                   tweet['public_metrics']['like_count'],
                                   tweet['public_metrics']['quote_count'],
                                   urls,domains,
                                   #json.dumps(tweet)
                                ])
                file.close()
        else:
            print("Empty result")
    print(count)
