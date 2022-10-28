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

search_url = "https://api.twitter.com/2/tweets/search/all"
bearer_token='AAAAAAAAAAAAAAAAAAAAAARzMQEAAAAABg5mjk7Sr0sSq8Gpyao9kboKhuE%3D0cNePyaHNsGyrUiYiacnGmDKMALOZCD7bJV8o41ReXJVyyblzx'
headers = create_headers(bearer_token)

file_name= '1. tweet di quotidiani.csv'
file = open(file_name, "w",encoding="utf-8")
csv_file=csv.writer(file,delimiter=",",quotechar="\"")

csv_file.writerow(['id','author_id','author_username','created_at','text','conversation_id','source',
                   'retweet_count','reply_count','like_count','quote_count','url','domain',
                   #'json'
                    ]
                  )

next_token = None
count = 0
while next_token != 0:
    query_params = {'query':
                        '(  (   '
                        '    ( "gap tra uomini e donne" OR "disparità salariale" OR "parità salariale" OR "partità salario" OR "#paritasalariale") '
                        '    OR'
                        '    ( "genere" '
                        '       ('
                        '         ('
                        '           (disuguaglianza OR uguaglianza OR divario OR gap OR disparità OR parità ) '
                        '           (carriera OR occupazione OR occupazionale OR lavoro)'
                        '         )'
                        '         OR '
                        '         ( '
                        '            (inferiore OR superiore OR uguale  OR pari OR parità OR disparità)'
                        '            (stipendio OR salario OR salariale OR retribuzione) '

                        '         )'
                        '       )'
                        '    )'
                        '    OR ("career gap" OR "pay gap" OR "wage gap" OR "gender pay gap" OR "gender gap" OR "#gendergap"  ) '

                        '    OR'
                        '    ( "maternità"'
                        '    ( lavoro OR stipendio OR salario) '
                        '         )'


                        '  ) '




                        '  ( from:Corriere'
                        ' OR from:repubblica'
                        ' OR from:MediasetTgcom24'
                        ' OR from:fanpage'
                        ' OR from:fattoquotidiano'
                        ' OR from:ilmessaggeroit'
                        ' OR from:ilgiornale'
                        ' OR from:virgilio_it'
                        ' OR from:LaStampa'
                        ' OR from:Agenzia_Ansa'
                        ' OR from:Libero_official'
                        ' OR from:sole24ore'
                        ' OR from:leggoit'
                        ' OR from:HuffPostItalia)'
                        ')'

                        ' lang:it '
                        ' -is:retweet '  # no retweets
                        ' -is:reply '  # no replies
                        ' -is:quote '  # no quotes
        ,
                    'max_results': 500,
                    'next_token': next_token,
                    'start_time': datetime.strftime(datetime(2017, 1, 1, 0, 0, 0), '%Y-%m-%dT%H:%M:%SZ'),
                    # 'end_time':  datetime.strftime(datetime(2022,5,26,10,0,0),    '%Y-%m-%dT%H:%M:%SZ'),
                    'tweet.fields': 'author_id,'
                                    'created_at,'
                                    'public_metrics,'
                                    'source,'
                                    'conversation_id,'
                                    'entities',
                    'expansions':
                        'author_id,'
                        'geo.place_id,'
                        'in_reply_to_user_id,'
                        'referenced_tweets.id,'
                        'referenced_tweets.id.author_id',
                    }
    status = 0
    while status != 200:
        try:
            status, json_response = connect_to_endpoint(search_url, headers, query_params)
            # print(status)
        except Exception as e:
            print(e)
            print(status)
            time.sleep(30)

        # print(json.dumps(json_response['meta']))
        if 'meta' in json_response:
            if 'next_token' in json_response['meta']:
                next_token = json_response['meta']['next_token']
            else:
                next_token = 0
        else:
            next_token = 0

        if 'data' in json_response:
            # print(json_response)
            print(len(json_response['data']), " tweets recovered")
            for tweet in json_response['data']:
                count += 1
                # print(json.dumps(tweet))
                # print(tweet['created_at'],tweet['text'],tweet['public_metrics'],)
                file = open(file_name, "a", encoding="utf-8")
                csv_file = csv.writer(file, delimiter=",", quotechar="\"", lineterminator='\n')
                # file.write(json.dumps(tweet)+"\n")
                urls = ''
                domains = ''
                if 'entities' in tweet:
                    if 'urls' in tweet['entities']:
                        # print(tweet)
                        urls = ','.join([t['url'] for t in tweet['entities']['urls']])
                        domains = ','.join([t['expanded_url'].split("/")[2] for t in tweet['entities']['urls']])

                author_screen_name = None
                for referenced_user in json_response['includes']['users']:
                    if str(referenced_user['id']) == str(tweet['author_id']):
                        # print(referenced_user)
                        author_screen_name = referenced_user['username']

                csv_file.writerow(["_" + str(tweet['id']),
                                   "_" + str(tweet['author_id']),
                                   author_screen_name,
                                   tweet['created_at'],
                                   tweet['text'],
                                   "_" + str(tweet['conversation_id']),
                                   tweet['source'],
                                   tweet['public_metrics']['retweet_count'],
                                   tweet['public_metrics']['reply_count'],
                                   tweet['public_metrics']['like_count'],
                                   tweet['public_metrics']['quote_count'],
                                   urls, domains,
                                   # json.dumps(tweet)
                                   ])
                file.close()
        else:
            print("Empty result")
print(count)

"""

'query': '( -musica -razzismo '
                     '  (   '
                     '    ( "gap tra uomini e donne" OR "disparità salariale" OR "parità salariale" OR "partità salariale" OR "divario retributivo" OR "parità retributiva" OR "differenze salariali" OR "#paritasalariale" ) '
                     '    OR'
                     '    ( "genere" '
                     '       ('
                     '         ('
                     '           (disuguaglianza OR uguaglianza OR divario OR gap OR disparità OR parità ) '
                     '           (carriera OR occupazione OR occupazionale OR lavoro)'
                     '         )'
                     '         OR '
                     '         ( '
                     '            (inferiore OR superiore OR uguale  OR parità OR disparità)'
                     '            (stipendio OR salario OR salariale OR retribuzione) '
                     '         )'
                     '       )'
                     '    )'
                     '    OR ("career gap" OR "pay gap" OR "wage gap" OR "gender pay gap" OR "gender gap" OR "#gendergap" ) '
                     '  ) '
                     ')'
                     ' lang:it '
                     ' -is:retweet ' #no retweets
                     ' -is:reply '   #no replies
                     ' -is:quote '   #no quotes
                     #' -has:links '  #no URLs
                     ' -has:media '  #no media
                     ' -has:images ' #no images
                     ' -has:videos ' #no videos
                     ' -is:nullcast '#no advertising tweets

"""
