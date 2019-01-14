# Clustering Similar Webpages
A SPARK pipeline implementation for clustering similar webpages.

Webpages are transformed into a vector that can be used to compute distances. This vector is used by LSH (locality sensitive hashing) to cluster large amounts of pages without the need to compare all webpages. Although one downside is the probabilistic nature of LSH.

When all pages are clustered the LSH model can be queried for similar pages and an exact distance measure is executed to prevent false positives

### Pipeline
The data goes through several steps before it can be used by LSH.
1) HTML is extracted from the web archive files.
2) A complete python object of the DOM is created from the html.
3) The DOM object is transformed into a simpler tree format.
4) The tree is transformed into a binary tree.
5) Tree nodes are transformed to 3-grams: <root, left, right> and the 3-grams are counted to form a vector.
6) The vector is used by LSH to do the hashing.

Finally the vector can be used to query the LSH model and exact distances can be computed using several distance measures (for example cosine). 

### Data
The data is available on the web archive in the `warc` file format. A subset of a month of data was used during testing.

### Details & References
A more detailed explanation is available in the PDF with references.