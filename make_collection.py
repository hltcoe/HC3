import argparse
import os
import re
import json
import gzip
from pathlib import Path
from tqdm.auto import tqdm

import logging
logging.basicConfig(
    format=f"[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=os.environ.get('LOGLEVEL', 'INFO').upper(),
)
logger = logging.getLogger("make_collection") 

def clean(text: str):
    # Compress out spaces and newlines
    return re.sub(r'\s+', ' ', str(text)).strip()

class JsonlLookupReader:
    def __init__(self, fn: str, id_key: str):
        self.fn = fn
        self.id_map = {}
        self.fptr = open(fn)

        self.build_id_location_map(id_key)

    def build_id_location_map(self, key):
        loc = 0
        mapping = {}
        with open(self.fn) as fr:
            line = fr.readline()
            while line:
                mapping[ json.loads(line)[key] ] = loc
                loc = fr.tell()
                line = fr.readline()

        self.id_map = mapping

    def __contains__(self, id: str):
        return id in self.id_map

    def __getitem__(self, id: str):
        self.fptr.seek(self.id_map[id])
        return json.loads(self.fptr.readline())

def read_reference_ids(fns):
    for fn in fns:
        with (gzip.open if fn.endswith('.gz') else open)(fn, 'rt') as fr:
            yield from map(json.loads, fr)

def main(args):
    assert not Path(args.output_file).exists() or args.overwrite, f"{args.output_file} already exists."

    tweet_reader = JsonlLookupReader(args.downloaded_tweets, 'tweetid')

    ndoc_created = 0
    ndoc_expected = 0
    with open(args.output_file, "w") as fw:
        for doc_info in tqdm(read_reference_ids(args.reference_doc_ids), dynamic_ncols=True):
            text = ""
            collected_tweets = []
            ntweets = 0
            fail = False
            for ask_lang, ask_twid in doc_info['tweet_ids']:
                if ask_twid not in tweet_reader and ask_lang == args.lang:
                    logger.debug(f"Missing {ask_twid} so discard {doc_info['doc_id']}")
                    fail = True
                    break

                if ask_twid in tweet_reader:                        
                    collected_tweets.append(tweet_reader[ask_twid])
                    collected_tweets[-1]['lang'] = ask_lang
                    if ask_lang == args.lang:
                        ntweets += 1
                        text += clean(collected_tweets[-1]['text'])
                        collected_tweets[-1]['text'] = ""

                text += "\n"
                
            if not fail:
                fw.write(json.dumps({
                    "id": doc_info['doc_id'],
                    "title": "",
                    "text": text,
                    "tweets": collected_tweets,
                    "numtweets": ntweets
                }) + '\n')
                ndoc_created += 1
            ndoc_expected += 1
    
    logger.info(f"Done -- created {ndoc_created} doc and expected {ndoc_expected}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Create collection file")
    parser.add_argument('--downloaded_tweets', type=str, help="jsonl file that stores the tweets", required=True)
    parser.add_argument('--reference_doc_ids', type=str, help="released doc and tweet ids", nargs='+', required=True)
    parser.add_argument('--lang', type=str, help="language", required=True)
    parser.add_argument('--output_file', type=str, help="output collection file", default="docs.jsonl")
    parser.add_argument('--overwrite', action='store_true', default=False)
    parser.add_argument('--verbose', action='store_true', default=False)

    main(parser.parse_args())