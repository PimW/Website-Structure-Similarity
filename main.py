#!/usr/bin/python3

from treesim.htmlextractor import HtmlExtractor
from treesim.htmlparser import HtmlParser
from treesim.treeparser import TreeParser
from treesim.treestringify import TreeStringifyTransformer
from treesim.btreeformatter import TreeFormatter

from pyspark import SparkConf, SparkContext
from datasketch import MinHash, MinHashLSH, WeightedMinHashGenerator
from scipy import spatial
from operator import add

import logging
import os
import time
import pickle
import argparse
import string
import numpy
import hashlib
from pprint import pprint


parser = argparse.ArgumentParser()

parser.add_argument('--query', help='Query url for lsh')
parser.add_argument('--train', choices=['test', 'all'])
args = parser.parse_args()

# SPARK SETUP
# environment should be set before spark context
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'ipython'

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)


# LOGGING SETUP
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

# CONFIG
# TODO: change this to use the amazon resource urls (that can be distributed)
files = [
    "data/CC-MAIN-20170116095120-00541-ip-10-171-10-70.ec2.internal.warc",
    "data/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.gz"
]

relevant_attrs = ['class', 'name']
filter_tags = [] #['span', 'p', 'script']
replacement_dict = {
    # tag: replacement
}

struct_tags_file = "./structuraltags.txt"
non_struct_tags_file = "./nonstructuraltags.txt"


def createReplacementDict():
    """
    Reads out a file of useful structural tags, which then are put in the replacement
    dictionary with generated strings to replace those. Strings are generated aa, ab, ac,
    etc.
    """
    i = 0
    with open(struct_tags_file) as stf:
        next(stf)  # skip the first line
        for line in stf:
            tag = line.split()[0][1:-1]  # take the tag, ignore the < and >
            if tag:
                replacement_dict[tag] = i
                i += 1


# Maybe consider using the 'structural tags' list which is also used in function
# createReplacementDict, as to ensure more consistency and less maintenance in case
# of 'new' tags to be added in any file.
def readNonStructuralTags():
    """
    Reads out the non-structural tags as given in provided file, and stores these in
    the tags filter which is used by the treeparser.
    """
    with open(non_struct_tags_file) as nstf:
        next(nstf)  # skip the first line
        for line in nstf:
            tag = line.split()[0][1:-1]  # take the tag, ignore the < and >
            filter_tags.append(tag)

wmg = None


def create_minhash(data):
    minhash = MinHash(HASH_SIZE, seed=12)
    for qgram in data:
        minhash.update(qgram.encode('utf-8'))
    return minhash


def create_weighted_minhash(data):
    global wmg
    if not wmg:
        wmg = WeightedMinHashGenerator(len(data), 128, seed=12)
    minhash = wmg.minhash(data)
    return minhash


# CONFIG
BATCH_SIZE = 100
MAX_FILES = 1000
HASH_SIZE = 128
THRESHOLD = 0.7
MODEL_FILE = 'lsh.pickle'
MATRIX_FILE = 'matrix.pickle'



def execute_pipeline(file):
    # PIPELINE
    # Add data and create spark RDD
    record_file = html_extractor.extract_warc_file(file)
    record_array = html_extractor.parse_warc_records(record_file, batch_size=BATCH_SIZE)

    record_count = 0
    for record_batch in record_array:
        print("Processing ({0}/{1})".format(record_count + len(record_batch), MAX_FILES))
        record_count += len(record_batch)
        recordsRDD = sc.parallelize(record_batch)

        # TODO: create version of pipeline with binary formatting of trees
        # Flow
        # html_extractor -> html_parser -> tree_parser -> tree_stringify -> hashing
        htmlRDD = recordsRDD.map(lambda x: html_parser.run(x))
        treeRDD = htmlRDD.map(lambda x: tree_parser.run(x))
        matrixRDD = treeRDD.map(lambda x: tree_to_matrix.run(x))
        minhashRDD = matrixRDD.map(lambda x: (x['url'], create_minhash(x['data']['qgrams']), x['data']['matrix']))  # MinHash(num_perm=128).update(x['data'])))

        minhash_tuples = minhashRDD.collect()

        for minhash_tuple in minhash_tuples:
            minhashes[minhash_tuple[0]] = (minhash_tuple[1], minhash_tuple[2])
            lsh.insert(minhash_tuple[0], minhash_tuple[1])
            print('Inserted url: {0}'.format(minhash_tuple[0]))

    sc.stop()

    with open(MODEL_FILE, 'wb') as f:
        f.write(pickle.dumps(lsh))

    with open(MATRIX_FILE, 'wb') as f:
        f.write(pickle.dumps(minhashes))

    # For debugging purposes
    """
    count = 0
    for url, minhash in minhashes.items():
        result = lsh.query(minhash[0])
        print("Similar pages to: {0}".format(url))
        for res in result:
            jaccard_similarity = minhash[0].jaccard(minhashes[res][0])
            cosine_similarity = spatial.distance.cosine(minhash[1], minhashes[res][1])

            print("\t{0}({1}) \t {2}".format(jaccard_similarity, cosine_similarity, res))

        count += 1
        if count > 10:
            break
    """


def query_lsh(url):
    with open(MODEL_FILE, 'rb') as f:
        lsh = pickle.loads(f.read())

    with open(MATRIX_FILE, 'rb') as f:
        minhashes = pickle.loads(f.read())

    record = html_extractor.record_from_url(url)

    record = tree_to_matrix.run(
        tree_parser.run(
            html_parser.run(record)
        )
    )

    minhash_tuple = (record['url'], create_minhash(record['data']['qgrams']), record['data']['matrix'])
    lsh.insert(minhash_tuple[0]+ '*', minhash_tuple[1])
    minhashes[minhash_tuple[0]+ '*'] = (minhash_tuple[1], minhash_tuple[2])
    result = lsh.query(minhash_tuple[1])

    print("Similar pages to: {0}".format(url))
    for res in result:
        jaccard_similarity = minhash_tuple[1].jaccard(minhashes[res][0])
        cosine_similarity = spatial.distance.cosine(minhash_tuple[2], minhashes[res][1])

        print("\t{0}({1}) \t {2}".format(jaccard_similarity, cosine_similarity, res))

if __name__ == '__main__':
    readNonStructuralTags()
    createReplacementDict()

    start_time = time.time()

    # INITALIZE PIPELINE
    # Initialize MinHashLSH
    lsh = MinHashLSH(threshold=THRESHOLD, num_perm=HASH_SIZE, weights=(0.8, 0.2))
    minhashes = {}  # store the urls that were minhashed with their minhashes

    if args.train:
        if args.train == 'test':
            MAX_FILES = 200
        elif args.train == 'all':
            MAX_FILES = 1000000
        else:
            print('Invalid argument for train')
            exit()

    # Transformations
    html_extractor = HtmlExtractor([], 'extract', max_files=MAX_FILES)
    html_parser = HtmlParser(['extract'], 'html_parse')
    tree_parser = TreeParser(relevant_attrs, filter_tags, replacement_dict, ['extract', 'html_parse'], 'tree_parse')
    tree_to_string = TreeStringifyTransformer(['extract', 'html_parse', 'tree_parse'], 'tree_stringify')
    tree_to_matrix = TreeFormatter(list(replacement_dict.values()), ['extract', 'html_parse', 'tree_parse'],
                                   'tree_to_matrix')

    if args.train:
        execute_pipeline(files[1])
    elif args.query:
        query_lsh(args.query)
    else:
        parser.print_help()


    print("\n\n--- %s seconds ---" % (time.time() - start_time))