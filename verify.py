import argparse
import gzip
import json
from hashlib import md5
from tqdm.auto import tqdm

def hash_text(text: str):
    return md5(text.encode()).hexdigest()


# Documenting the generation function -- not for running
def __create_release_tweet_ids(doc_fn: str, output_fn: str):
    # release doc id with tweet ids
    with gzip.open(output_fn, 'wt') as fw:
        for l in tqdm(open(doc_fn)):
            l = json.loads(l)
            fw.write(json.dumps({
                "doc_id": l['id'], 
                "tweet_ids": [ (t['lang'], t['tweetid']) for t in l['tweets'] ],
                "doc_hash": hash_text(l['text']),
                "date": l['date']
            }) + "\n")


class ErrorHandler:
    def __init__(self, args):
        self.early_stop: bool = args.early_stop
        self.n_error = 0

    def output_error(self, message):
        tqdm.write(f"[ERROR] {message}")
        self.n_error += 1
        if self.early_stop:
            exit(1)


def verify(args):
    print("Reading id files...")
    ids = { 
        d['doc_id']: d 
        for fn in args.id_files
        for d in map(json.loads, tqdm(gzip.open(fn, 'rt'), desc=fn, dynamic_ncols=True)) 
    }

    handler = ErrorHandler(args)
    opener = (gzip.open if args.doc_file.endswith('.gz') else open)
    found_doc = set()
    for l in tqdm(opener(args.doc_file, 'rt'), dynamic_ncols=True, total=len(ids)):
        l = json.loads(l)
        if l['id'] not in ids:
            handler.output_error(f"Cannot find doc `{l['id']}`")
            continue
        
        ref = ids[ l['id'] ]
        hash = hash_text(l['text'])
        if hash != ref['doc_hash']:
            handler.output_error(f"Hash mismatch for doc `{l['id']}`, got {hash} expect {ref['doc_hash']}")
            continue
        
        found_doc.add(l['id'])
    
    if len(found_doc) != len(ids):
        handler.output_error(f"Found {len(found_doc)} docs but expect {len(ids)}.")
        missing_ids = ids.keys() - found_doc
        if len(missing_ids) < 200:
            print(f"Missing these docs: {missing_ids}")

    print(f"Found {handler.n_error} errors.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser("Verify the document jsonl file")
    parser.add_argument('--doc_file', help="path to document jsonl file", type=str)
    parser.add_argument('--id_files', help="path to the id files", nargs='+', type=str)
    parser.add_argument('--early_stop', action='store_true', help="stop at first error", default=False)

    verify(parser.parse_args())