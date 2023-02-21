# HC3 Collection

This repository contains scripts for
- Downloading and assembling the document collection
- Removing unavailable documents from run files and qrels

And other collection resources, including 
- Topic `jsonl` files (`resources/{train,eval}.topics.jsonl`)
- Qrels files containing the relevance judgments (`resources/{zho,fas}.{train,eval}.qrels` )
- TREC run files of all baseline runs reported in the paper (`run_files/*.trec`)

## Assembing Document Collections

To assemble the document collection, please download the Tweets via Twitter API 2.0. 
We provide the scripts for downloading Tweets, constructing the document `jsonl` file, and verify the MD5 hashs of the documents. 

The scripts are light weight Python scripts that only requires `requsts` and `tqdm` to run. 

### Get Started

Please provide the Bearer tokens of your Twitter Developer Project through environment variable `BEARER_TOKEN`.
```bash
export BEARER_TOKEN=<your bearer token>
```

### Download

Please extract the Tweet IDs from the `resouces/{zho,fas}.doc_tweet_ids.*.jsonl.gz` via
```bash
./extract.tweet.ids.sh ./resources/$lang.doc.tweet.ids.jsonl.gz > $lang.tweet.ids.txt
```
or simply 
```bash
zcat ./resources/$lang.doc.tweet.ids.*.jsonl.gz | jq -cr '.tweet_ids[] | .[1]' > $lang.tweet.ids.txt
```
or anything that put one required Tweet IDs each line of the file. 

The following command will download the Tweets, passing an exisitng file to the `--tweet_output` tag will resume the download
and not redownloading the existing Tweets. 
```bash
python download_tweets.py --tweetlist $lang.tweet.ids.txt \
                          --tweet_output ./downloads/$lang.tweets.jsonl \
                          --error_output ./downloads/err.log
```

### Construct the document jsonl file

The following command will assemble the individual Tweets into conversations. 
Note that missing any Tweet in the specified language will result in dropping the whole conversation. 

```bash
python make_collection.py --downloaded_tweets ./downloads/$lang.tweets.jsonl \
                          --reference_doc_ids ./resources/$lang.doc.tweet.ids.*.jsonl.gz \
                          --lang $lang \
                          --output_file ./downloads/$lang.docs.jsonl
```

### Verify

Please use the following command to verify the collection you assembled. 
```bash
python verify.py --doc_file ./downloads/$lang.test.docs.jsonl --id_files ./resources/$lang.doc.tweet.ids.*.jsonl.gz 
```

## Excluding Unavailable Documents

To ensure comparability, please use the `filter_docs.py` script to exclude documents that are not available in your collection. 
This script can filter both run files and qrels. 
Please provide a text file that contains all available document ids (one in each line) or simply pass in your document `jsonl` file through `--ids`. 

The following command is an example 
```bash
python filter_docs.py --ids your-zho-docs.jsonl --runs ./run_files/zho.comb.BM25-QHT.trec 
```

## Citation

TBA