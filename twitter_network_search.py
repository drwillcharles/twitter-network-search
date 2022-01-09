import os
import requests
import json
import pandas as pd

import time
from datetime import datetime

# Make sure the environment variable is already set up
bearer_token = os.environ.get("BEARER_TOKEN")


def extract_public_metrics(df_tweets):
    '''
    Pulls out the `public_metrics` object and appends this to the Pandas dataframe as separate columns.
    '''
    if 'public_metrics' in df_tweets.columns:
        public_metric_columns = ['retweet_count', 'reply_count', 'like_count', 'quote_count']
    
        for public_metric in public_metric_columns:
            df_tweets[public_metric] = df_tweets['public_metrics'].apply(lambda x: x[public_metric])
            
        df_tweets = df_tweets.drop(columns=['public_metrics'])
    
    return df_tweets

def extract_referenced_tweets(df_tweets):
    '''
    Pulls out the `referenced_tweets` object and appends this to the Pandas dataframe as separate columns.
    '''
    if 'referenced_tweets' in df_tweets.columns:
        df_tweets['type'] = df_tweets['referenced_tweets'].apply(lambda x: x[0]['type'] if isinstance(x, list) else None)
        df_tweets['referenced_tweet_id'] = df_tweets['referenced_tweets'].apply(lambda x: x[0]['id'] if isinstance(x, list) else None)
        
        df_tweets = df_tweets.drop(columns=['referenced_tweets'])
    
    return df_tweets    

def clean_tweets_dataframe(df_tweets):
    '''
    Clean up dataframe object obtained from REST API JSON
    '''
    df_tweets = extract_public_metrics(df_tweets)
    df_tweets = extract_referenced_tweets(df_tweets)
    
    return df_tweets

def tweets_url(ids:list):
    tweet_fields = 'id,author_id,public_metrics,conversation_id,created_at' #,in_reply_to_user_id #,entities
    user_fields = 'name,username,profile_image_url'
    expansions = 'author_id,referenced_tweets.id,in_reply_to_user_id,referenced_tweets.id.author_id'
    
    url = f"https://api.twitter.com/2/tweets?ids={','.join(ids)}"+\
        f"&tweet.fields={tweet_fields}"+\
        f"&user.fields={user_fields}"+\
        f"&expansions={expansions}"
    
    return url

def tweet_url(tweet_id:str):
    '''
    Pulls data for an individual tweet. You can adjust ids to include a single Tweets
    or add to up to 100 comma-separated IDs
    '''
    tweet_fields = "tweet.fields=lang,author_id"
    
    ids = "ids="+tweet_id

    url = "https://api.twitter.com/2/tweets?{}&{}".format(ids, tweet_fields)
    return url

def search_url(query:str, max_results:int=100, start_time=None, end_time=None) -> str:
    '''
    Generates endpoint for Twitter REST API: GET /2/tweets/search/recent
    Time format must be in RFC 3339 UTC timestamp eg `2022-01-04T00:00:00.000Z`

    '''
    
    tweet_fields = 'id,author_id,public_metrics,conversation_id,created_at' #,in_reply_to_user_id #,entities
    user_fields = 'name,username,profile_image_url'
    expansions = 'author_id,referenced_tweets.id,in_reply_to_user_id,referenced_tweets.id.author_id'
    url = f"https://api.twitter.com/2/tweets/search/recent"+\
        f"?query={query} -is:reply -is:quote"+\
        f"&max_results={max_results}"+\
        f"&tweet.fields={tweet_fields}"+\
        f"&user.fields={user_fields}"+\
        f"&expansions={expansions}"
    
    if start_time is not None:
        url+=f"&start_time={start_time}"
    if start_time is not None:
        url+=f"&end_time={end_time}"
        
    return url

def replies_to_user_url(user_id:str, max_results:int=100) -> str:
    '''
    Generates endpoint for Twitter REST API: GET /2/tweets/search/recent
    Gets all replies to an individual user_id

    '''
    tweet_fields = 'id,author_id,public_metrics'
    user_fields = 'name,username,profile_image_url'
    expansions = 'author_id,referenced_tweets.id,in_reply_to_user_id,referenced_tweets.id.author_id'
    
    url = f"https://api.twitter.com/2/tweets/search/recent?query=to%3A{user_id}%20OR%20retweets_of%3A{user_id}"+\
        f"&max_results={max_results}"+\
        f"&tweet.fields={tweet_fields}"+\
        f"&user.fields={user_fields}"+\
        f"&expansions={expansions}"
    
    return url


def liking_users_url(tweet_id):

    '''
    '''
    url = f"https://api.twitter.com/2/tweets/{tweet_id}/liking_users"
    return url

def retweeting_users_url(tweet_id):

    url = f"https://api.twitter.com/2/tweets/{tweet_id}/retweeted_by"
    return url
    
def user_url(user_id):
    url = f"https://api.twitter.com/2/users/{user_id}"
    return url

def get_conversation_url(conversation_id, max_results=100):
    '''
    Get all comments and replies related to this tweet
    '''
    tweet_fields = 'id,author_id,public_metrics,conversation_id,created_at' #,in_reply_to_user_id #,entities
    user_fields = 'name,username,profile_image_url'
    expansions = 'author_id,referenced_tweets.id,in_reply_to_user_id,referenced_tweets.id.author_id'
    url = f"https://api.twitter.com/2/tweets/search/recent"+\
        f"?query=conversation_id:{conversation_id}"+\
        f"&max_results={max_results}"+\
        f"&tweet.fields={tweet_fields}"+\
        f"&user.fields={user_fields}"+\
        f"&expansions={expansions}"
        
    return url

def bearer_oauth(r):
    '''
    Method required by bearer token authentication.
    '''
    r.headers['Authorization'] = f"Bearer {bearer_token}"
    return r


def connect_to_endpoint(url, wait_on_timeout=True):
    response = requests.request("GET", url, auth=bearer_oauth)
    
    epochtime = response.headers['x-rate-limit-reset']
    rate_limit_reset_time = datetime.fromtimestamp(int(epochtime))
    rate_limit_remaining = response.headers['x-rate-limit-remaining']

    print(f"{response.status_code}\tx-rate-limit-remaining: {rate_limit_remaining}\tx-rate-limit-reset: {rate_limit_reset_time}")
    
    # If the REST API limit is reached, we can sleep until the limit is reset and then continue
    if response.status_code == 429 and wait_on_timeout == True:
        rate_limit_reset_time = datetime.fromtimestamp(int(epochtime))
        time_difference = rate_limit_reset_time-datetime.now()
        print(f"Rate limit resets at {rate_limit_reset_time}. Sleeping for {time_difference.seconds} seconds...")
        time.sleep(time_difference.seconds+10)
        print(datetime.now())
        response = requests.request("GET", url, auth=bearer_oauth)
        
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

def day_to_time(day:str) -> str:
    '''
    Convert `dd-mon-yyyy` format to UTC timestamp. Used to generate Twitter API url.
    '''
    if day is not None:
        time = datetime.strptime(day,'%d-%b-%Y').isoformat() + "Z"
    else:
        time = None
    
    return time

def search_and_paginate(query:str, num_results:int=100, wait_on_timeout=True, start_day=None, end_day=None):
    '''
    Calls the Twitter REST API /2/tweets/search/recent and paginates results
    :return: Tweets as a Pandas dataframe.

    '''
    
    results_each_call = 100
    
    # Must be RFC 3339 UTC timestamp
    start_time = day_to_time(start_day)
    end_time = day_to_time(end_day)
    
    url = search_url(query, results_each_call,start_time,end_time)
    
    json_response = connect_to_endpoint(url, wait_on_timeout)
        
    df_tweets = pd.DataFrame(json_response['data'])
    df_users = pd.DataFrame(json_response['includes']['users'])
    
    # As maximum results for each call is 100, more REST API calls may need to be made to collect
    # more results.
    while 'next_token' in json_response['meta'] and len(df_tweets) < num_results:
        pagination_token = json_response['meta']['next_token']
        
        json_response = connect_to_endpoint(f'{url}&next_token={pagination_token}', wait_on_timeout)
        
        df_tweets = df_tweets.append(pd.DataFrame(json_response['data']),ignore_index=True)
        df_users = df_users.append(pd.DataFrame(json_response['includes']['users']),ignore_index=True)
    
    df_tweets = clean_tweets_dataframe(df_tweets)
    
    return df_tweets, df_users

    


def get_original_tweets(df_tweets, df_users):
    '''
    If the retweeted tweet is not in the original list, grab this as well and append it to the Pandas dataframe.
    Can probably do one call for multiple `conversation_id`s
    '''
    df_referenced_tweets = pd.DataFrame()
    df_referenced_users = pd.DataFrame()
    
    # Get tweets that reference tweets not in the list
    ids=df_tweets[(~df_tweets['referenced_tweet_id'].isin(df_tweets['id'])) & (df_tweets['type']=='retweeted')]['referenced_tweet_id'].tolist()
    
    #drop duplicates
    ids = list(dict.fromkeys(ids))
    
    # As a maximum of 100 tweets can be called, list needs to be split into chunks
    chunks = [ids[x:x+100] for x in range(0, len(ids), 100)]
    
    for chunk in chunks:
        url = tweets_url(chunk)
        json_response = connect_to_endpoint(url)

        df_referenced_tweets = df_referenced_tweets.append(pd.DataFrame(json_response['data']), ignore_index=True)
        df_referenced_users = df_referenced_users.append(pd.DataFrame(json_response['includes']['users']), ignore_index=True)
    
    df_referenced_tweets = clean_tweets_dataframe(df_referenced_tweets)

    df_tweets = df_tweets.append(df_referenced_tweets)
    df_users = df_users.append(df_referenced_users)
    
    return df_tweets, df_users

def get_conversations(df_tweets, df_users):
    '''
    Get all replies/quotes related to a conversation and append to the dataframes
    '''
    df_related_tweets = pd.DataFrame()
    df_related_users = pd.DataFrame()
    
    for index, tweet in df_tweets.iterrows():
        if tweet['reply_count'] > 0 and tweet['type'] is None:#  or tweet['quote_count'] > 0:
                
            url = get_conversation_url(tweet['conversation_id'])
            json_response = connect_to_endpoint(url)
            if json_response['meta']['result_count'] > 0:
                df_related_tweets = df_related_tweets.append(pd.DataFrame(json_response['data']), ignore_index=True)
                df_related_users = df_related_users.append(pd.DataFrame(json_response['includes']['users']), ignore_index=True)
            else:
                pass
    
    df_related_tweets = clean_tweets_dataframe(df_related_tweets)

    df_tweets = df_tweets.append(df_related_tweets)
    df_users = df_users.append(df_related_users)

    return df_tweets, df_users

    
def get_relationship(url, df_relationship, tweet_id):
    '''
    Obtains a table of Likes or Retweets. Can be used for both df_likes and df_retweets objects.
    '''
    json_response = connect_to_endpoint(url, wait_on_timeout=True)
    if 'errors' in json_response.keys():
        print(f"{json_response['errors'][0]['title']}: {json_response['errors'][0]['detail']}")    
    else:
        if json_response['meta']['result_count'] > 0:
            
            relationships = json_response['data']
            
            return df_relationship.append(pd.DataFrame(relationships).assign(relationship_tweet_id=tweet_id), 
                                          ignore_index=True)
    return df_relationship

def get_likes(df_tweets):
    '''
    Used to obtain a dataframe of users that liked the tweets.
    Returns a dataframe of relationships df_likes.
    '''
    df_likes = pd.DataFrame()
    
    for index, tweet in df_tweets.iterrows():
        
        if tweet['like_count'] >= 1:
            url = liking_users_url(tweet['id'])
            df_likes = get_relationship(url, df_likes, tweet['id'])
            
    return df_likes

def estimated_number_of_results(query, start_day=None, end_day=None):
    '''
    Before running the program it may be good to check how many results could be retrieved
    '''
        
    # Must be RFC 3339 UTC timestamp
    start_time = day_to_time(start_day)
    end_time = day_to_time(end_day)

    url = f"https://api.twitter.com/2/tweets/counts/recent?query={query}&granularity=day"
    if start_time is not None:
        url+=f"&start_time={start_time}"
    if end_time is not None:
        url+=f"&end_time={end_time}"

    json_response = connect_to_endpoint(url, False)
    print(f"{json_response['meta']['total_tweet_count']} results expected")

    return json_response

def main(query, num_of_results, output_path):
    '''
    Runs a Twitter Search and generates CSV files df_tweets and df_users.
    '''

    # By default the last 7 days are retrieved
    start_day = None # start_day = '04-Jan-2022'
    end_day = None # end_day = '05-Jan-2022'

    # Start collecting data

    df_tweet_search, df_user_search = search_and_paginate(query, num_results=num_of_results, wait_on_timeout=True, start_day=start_day, end_day=end_day)

    # If a retweet is found but not the original. Use this to find the original
    df_tweets, df_users = get_original_tweets(df_tweet_search, df_user_search)

    # Get replies from the conversation_id
    tweets_with_replies = len(df_tweets[df_tweets['reply_count'] > 0])
    print(f"{tweets_with_replies} API calls will need to be made. Due to API limits this may take {(tweets_with_replies//450)*15} additional minutes.")
    print(f"The time is now {datetime.now()}")

    df_tweets, df_users = get_conversations(df_tweets, df_users)

    # Remove duplicates
    df_users = df_users.drop_duplicates()

    print(f"Results:\n{len(df_tweets)} Tweets\n{len(df_users)} Users")

    # Save to CSV file
    df_tweets.to_csv(output_path+'df_tweets.csv')
    df_users.to_csv(output_path+'df_users.csv')

if __name__ == '__main__':
       # Define query here
    query = 'drug discovery'
    num_of_results = 300
    output_path = 'output/'

    main(query, num_of_results, output_path)