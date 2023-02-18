import argparse
import gzip
import json
from tqdm.auto import tqdm

def get_inclusive_ids(fn: str):
    opener = gzip.open if fn.endswith(".gz") else open

    # test which kind of file it is
    first_line = opener(fn, 'rt').readline()
    if first_line.startswith("{"): # jsonl
        return set([ json.loads(l)['id'] for l in tqdm(opener(fn, 'rt')) ])
    else: # text file
        return set([ l.strip() for l in tqdm(opener(fn, 'rt')) ])


def read_trec_run(line: str):
    line = line.split()
    return (
        # topic_id, "Q0", doc_id, rank, score, run_name
        line[0], line[1], line[2], int(line[3]), float(line[4]), line[5]
    )


def main(args):
    ids = get_inclusive_ids(args.ids)
    print(f"Found {len(ids)} docs")

    for run in args.runs: 
        raw_results = {}
        for line in map(read_trec_run, open(run)):
            if line[0] not in raw_results:
                raw_results[ line[0] ] = []
            raw_results[ line[0] ].append(line)
        
        with open(run+'.filtered', 'w') as fw:
            for topic_id, docs in raw_results.items():
                filtered_docs = sorted(filter(lambda d: d[2] in ids, docs), key=lambda x: x[4])[::-1]
                
                nremoved = len(docs)-len(filtered_docs)
                if nremoved > 0:
                    print(f"Topic {topic_id} drops {nremoved} docs from file {run}")
                
                for i, doc in enumerate(filtered_docs):
                    fw.write(f"{topic_id} {doc[1]} {doc[2]} {i} {doc[4]} {doc[5]}\n")

    for qrels in args.qrels:
        with open(qrels+'.filtered', 'w') as fw:
            nremoved = 0
            for line in open(qrels):
                if line.split()[2] not in ids:
                    nremoved += 1
                else:
                    fw.write(line)
            print(f"Drop {nremoved} entries from file {qrels}")



if __name__ == '__main__':
    parser = argparse.ArgumentParser("Filter documents that no longer exist from the run file")
    parser.add_argument('--ids', 
                        help="either a text file with inclusive doc id in each line or the document jsonl file",
                        type=str)
    parser.add_argument('--runs', help="input run files for filtering", type=str, nargs='+', default=[])
    parser.add_argument('--qrels', help="input qrels files for filtering", type=str, nargs='+', default=[])

    args = parser.parse_args()
    if len(args.runs) == 0 and len(args.qrels) == 0:
        print("Nothing to filter")
    else:
        main(args)