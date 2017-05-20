from treesim.htmlextractor import HtmlExtractor
from treesim.htmlparser import HtmlParser
from treesim.treeparser import TreeParser
from treesim.treestringify import TreeStringifyTransformer
from treesim.btreeformatter import TreeFormatter

import pickle
from datasketch import MinHash, MinHashLSH

model_file = 'lsh.pickle'

with open(model_file, 'rb') as f:
    lsh = pickle.loads(f.read())

print(lsh)