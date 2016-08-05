# Word2Vec importer procedure

This procedure allows word and phrase embeddings from the
[Word2Vec tool](https://code.google.com/p/word2vec/) to be loaded
into MLDB.

Using these embeddings, each word or phrase in a language is convertible
to a multi-dimensional set of coordinates (typically hundreds of coordinates
are used).  This allows for natural language to be represented in a form
that is compatible with standard classification or clustering algorithms.

## Configuration

![](%%config procedure import.word2vec)

The `dataFileUri` parameter should point to a data file that is produced
by the `word2vec` tool.  A good default, containing the 3 billion most
frequent words and phrases for English, is available
[here](https://drive.google.com/file/d/0B7XkCwpI5KDYNlNUTTlSS21pQmM/edit?usp=sharing)
(warning: it is a 1.5 GB download), or directly [here](https://doc-0g-8s-docs.googleusercontent.com/docs/securesc/ha0ro937gcuc7l7deffksulhg5h7mbp1/g4gppkmj8r3k019aau67238mkj789i6m/1444910400000/06848720943842814915/*/0B7XkCwpI5KDYNlNUTTlSS21pQmM?e=download).

The file should be copied to a local file system or a high-bandwidth
service, and optionally decompressed, before being opened from MLDB.  MLDB will
require around 8GB of memory to hold the entire file in an `embedding` dataset.

The `limit` parameter allows only the first n words of a file to be loaded.
This is useful for when only embeddings for the most frequent words are
required.

The `offset` parameter allows a number of words to be skipped.  This is
useful when loading multiple datasets in parallel.

## Example

Sample query to load the word2vec dataset into an "embedding" dataset
type and determine the closest words to "France".

```python
mldb.put("/v1/procedures/w2vimport", {
    "type": "import.word2vec",
    "params": {
        "dataFileUrl": "file:///path/to/GoogleNews-vectors-negative300.bin",
        "output": "w2v",
        "limit": 100000
    }
})

mldb.put("/v1/functions/w2v_neighbors", {
    "type": "embedding.neighbors",
    "params": {
        "dataset": "w2v"
    }
})

mldb.query("SELECT w2v_neighbors({coords: 'France'})[neighbors] as *")
```

This gives the output:

|*rowName*| 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
|---------|---|---|---|---|---|---|---|---|---|---|
| result | France | Belgium | French | Germany | Paris | Spain| Italy|Europe| Morocco | Switzerland |

# See also

* The ![](%%doclink embedding.neighbors function) is used to get the nearest neighbor rows in an existing embedding dataset
* The ![](%%doclink pooling function) is used to embed a bag of words in a vector space like Word2Vec
* The ![](%%doclink embedding dataset) is the perfect dataset to hold
  the output of the word2vec tool.
* The [Word2Vec tool](https://code.google.com/p/word2vec/) project page
  contains source code to train your own embeddings.
