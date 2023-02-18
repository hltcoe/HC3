import sys
import requests
import os
import json
import time
import argparse
import gzip
from pathlib import Path

import logging
logging.basicConfig(
    format=f"[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=os.environ.get('LOGLEVEL', 'INFO').upper(),
)
logger = logging.getLogger("download_tweets") 

search_url = "https://api.twitter.com/2/tweets/search/all"

LONG_WAIT = 60
MAX_IDS_PER_15 = 300
MAX_CONV_PER_15 = 300
TWEET_PULL_LIMIT = 10000000
ID_LIMIT = 100
MAX_PULL_TRY = 10
DEF_WAIT = 2

def auth():
    return os.environ.get("BEARER_TOKEN")

def _anyopen(fn: str, *args, **kwargs):
    return (gzip.open if str(fn).endswith('.gz') else open)(fn, *args, **kwargs)


def getfield(tweet, *fields):
    for field in fields:
        if field not in tweet:
            return None
        tweet = tweet[field]
    return tweet

def create_tweet_url(tweet_ids):
    assert(len(tweet_ids) <= ID_LIMIT)
    tweet_fields = "tweet.fields=lang,author_id,created_at,conversation_id,referenced_tweets,context_annotations,entities,in_reply_to_user_id"
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld

    ids = "ids=" + ','.join(tweet_ids)
    # You can adjust ids to include a single Tweets.
    # Or you can add to up to 100 comma-separated IDs
    url = "https://api.twitter.com/2/tweets?{}&{}".format(ids, tweet_fields)
    return url


def read_jsonl(path):
    with _anyopen(path) as f:
        for line in f:
            if line.strip():
                try:
                    json_data = json.loads(line)
                    if 'EVENT_ID' in json_data:
                        event_id = json_data['EVENT_ID']
                        logger.warning(f'Reading {event_id}')
                    yield json_data
                except Exception as e:
                    logger.warning(f'cannot load data in {line}')


def write_jsonl(items, path, mode='a'):
    assert mode in ['w', 'a']
    with open(path, mode) as f:
        for x in items:
            f.write(json.dumps(x) + '\n')

def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers

KNOWN_CODES = [429, 503]
def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    #print(response.status_code)
    if response.status_code != 200:
        if response.status_code in KNOWN_CODES:
            logger.warning(f'got a twitter error of {response.status_code} ({response.text})')
            return -1
        else:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
    return response.json()

def parse_tweet(tweet):
    # Skip tweets that have no ID
    if 'id' not in tweet:
        return None

    # Text is in extended_tweet if available, otherwise in text
    text = None
    if 'text' in tweet:
        text = tweet['text']
    if 'extended_tweet' in tweet and 'full_text' in tweet['extended_tweet']:
        text = tweet['extended_tweet']['full_text']
    if text is None:
        return None
                    
    # Map all whitespace to a single space
    text = " ".join(text.split())
    
    # handle silent error of acount suspension
    if text.endswith("account is temporarily unavailable because it violates the Twitter Media Policy. Learn more."):
        logger.warning(f"TEMP UNAVAILABLE {tweet['id']}")
        return None

    # Calculate the date of the tweet, and build docno, date, and url from it
    # date = datetime.date.fromtimestamp(int(tweet['timestamp_ms']) / 1000.)
    # datestring = f"{date.year}_{date.month:02}_{date.day:02}"
    date = tweet["created_at"].split('T')[0] if "created_at" in tweet and 'T' in tweet["created_at"] else None
    
    return {
        "tweetid": tweet['id'],
        "authorid": tweet['author_id'],
        "text": text,
        "lang": tweet['lang'],
        "date": date,
        "url": f"twitter.com/anyuser/status/{tweet['id']}"
    }


def batching(it, batchsize: int): 
    ret = []
    for item in it:
        ret.append(item)
        if len(ret) >= batchsize:
            yield ret
            ret = []
    if len(ret) > 0:
        yield ret


# rate limit: 300 tweet lookups and 300 full-archive searches in 15 minutes
# 10 Million per month
def main(args):
    bearer_token = auth()
    headers = create_headers(bearer_token)
    first_pull = True

    # Collect tweets arleady pulled
    known_tweets = set()
    if args.error_input:
        for filename in args.error_input:
            logger.info(f'Processing file {filename}', )
            try:
                for err_tw in read_jsonl(filename):
                    known_tweets.add(err_tw['value'])
            except Exception as e:
                logger.warning(e, )
                logger.warning(err_tw, )
                logger.warning(f'FYI Need to fix the error file {filename}', )
                sys.exit(2)

    Path(args.tweet_output).parent.mkdir(parents=True, exist_ok=True)
    if Path(args.tweet_output).exists():
        logger.warning(f"File {args.tweet_output} already exists, will resume from it.")
        known_tweets |= { l['tweetid'] for l in map(json.loads, _anyopen(args.tweet_output)) }

    tweet_ids = [
        l for l in map(str.strip, _anyopen(args.tweetlist, 'rt'))
        if l not in known_tweets
    ]

    logger.info(f'There are {len(tweet_ids)} to process')
    assert len(tweet_ids) > 0, 'no tweets to process...exiting'

    begin_time = time.time()
    for tweet_batch in batching(tweet_ids, ID_LIMIT):

        logger.info(f'Processing the set next of {len(tweet_batch)} tweets ending with {tweet_batch[-1]}')
        url = create_tweet_url(tweet_batch)
        successful_pull = False
        pull_count = 0
        while not successful_pull:
            tweet_response = connect_to_endpoint(url, headers)
            pull_count += 1
            if tweet_response == -1:
                if first_pull:
                    logger.error('ERROR: first pull failes - inducing cronjob to halt', )
                    write_jsonl(tweet_ids[-10:-1], args.error_output)
                    return
                if pull_count >= MAX_PULL_TRY:
                    logger.warning('ran out of pull tries', )
                    return
                sleep_amt = LONG_WAIT - (time.time() - begin_time)
                if sleep_amt < DEF_WAIT:
                    sleep_amt = DEF_WAIT
                logger.warning(f'FYI - resting for {sleep_amt} seconds', )
                time.sleep(sleep_amt)
                begin_time = time.time()
            else:
                successful_pull = True
            first_pull = False

        time.sleep(DEF_WAIT)  # Can only do one search per second

        if 'errors' in tweet_response:
            valid_errors = []
            for err_tw in tweet_response['errors']:
                try:
                    if 'resource_id' in err_tw:
                        known_tweets.add(err_tw['resource_id'])
                        valid_errors.append(err_tw)
                    elif 'value' in err_tw:
                        known_tweets.add(err_tw['value'])
                        valid_errors.append(err_tw)
                except Exception as e:
                    logger.warning(e, )
                    logger.warning(f'FYI NEED TO FETCH {err_tw}')

            if args.error_output:
                write_jsonl(valid_errors, args.error_output)

        if 'data' in tweet_response:
            write_tweets = []
            for tw in tweet_response['data']:
                parsed = parse_tweet(tw) if not args.raw_tweets else tw
                if parsed is not None:
                    known_tweets.add(tw['id'])
                    write_tweets.append(parsed)
            write_jsonl(write_tweets, args.tweet_output)


if __name__ == "__main__":
    assert auth() != None, "Require BEARER_TOKEN to be in the environment variable"

    parser = argparse.ArgumentParser("Download individual tweets")
    parser.add_argument('--tweetlist', help="files containing tweet ids", type=str, required=True)
    parser.add_argument('--tweet_output', help="filename to write tweets", type=str, required=True)
    parser.add_argument('--raw_tweets', help="save the raw tweets", action='store_true', default=False)

    parser.add_argument('--error_input', help="files of previously pulled tweets with errors", type=str, nargs='*')
    parser.add_argument('--error_output', help="filename to write tweets that responded with an error", type=str)
    main(parser.parse_args())
