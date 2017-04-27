#!/usr/bin/python3

from treesim.htmlextractor import HtmlExtractor
from treesim.htmlparser import HtmlParser
from treesim.treeparser import TreeParser
from treesim.treestringify import TreeStringifyTransformer


from pyspark import SparkConf, SparkContext
from operator import add
import logging
import os
import hashlib
import time


# SPARK SETUP
# environment should be set before spark context
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'ipython'

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)


# LOGGING SETUP
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.WARNING)

# CONFIG
# TODO: change this to use the amazon resource urls (that can be distributed)
files = [
    "data/CC-MAIN-20170116095120-00541-ip-10-171-10-70.ec2.internal.warc",
    "data/CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc.gz"
]

relevant_attrs = ['class', 'name']
filter_tags = ['span', 'p', 'script']
replacement_dict = {
    # tag: replacement
}


def execute_pipeline(file):
    # PIPELINE

    # Transformations
    html_extractor = HtmlExtractor([], 'extract', max_files=2000)
    html_parser = HtmlParser(['extract'], 'html_parse')
    tree_parser = TreeParser(relevant_attrs, filter_tags, replacement_dict, ['extract', 'html_parse'], 'tree_parse')
    tree_to_string = TreeStringifyTransformer(['extract', 'html_parse', 'tree_parse'], 'tree_stringify')

    # Add data and create spark RDD
    record_array = html_extractor.extract_warc_file(file)
    record_array = html_extractor.parse_warc_records(record_array, batch_size=10)
    recordsRDD = sc.parallelize(record_array)

    # Flow
    # html_extractor -> html_parser -> tree_parser
    htmlRDD = recordsRDD.map(lambda x: html_parser.run(x))
    treeRDD = htmlRDD.map(lambda x: tree_parser.run(x))
    stringRDD = treeRDD.map(lambda x: tree_to_string.run(x))

    # Count equal html trees
    counts = stringRDD.map(lambda x: x['data']) \
        .map(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .map(lambda x: (x[1], x[0])) \
        .sortByKey()

    # Execute Pipeline
    #final_records = stringRDD.collect() #treeRDD.take(10)
    #for r in final_records:
    #    print(r['url'])
    #    print(r['data'])
    #    print('----------------------------------------------------')

    output = counts.collect()
    for (count, word) in output:
        print("{0:>5}: {1}".format(count, word))

    sc.stop()



if __name__ == '__main__':
    # TODO: parallelize, the result of extracting warc files is a buffered file and not thread safe
    start_time = time.time()
    execute_pipeline(files[1])
    print("\n\n--- %s seconds ---" % (time.time() - start_time))
