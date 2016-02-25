# SentiWordNet Importer Procedure

This procedure allows word and phrase embeddings from the
[SentiWordNet lexical resource](http://sentiwordnet.isti.cnr.it) to be loaded
into MLDB. 


Using these embeddings, each word or phrase in English is convertible
to a 3-dimensional set of coordinates representing sentiment scores: 
positivity, negativity, objectivity.

This is a simple implementation that does not do word sense disambiguation.
SentiWordNet provides sentiment scores for each of 
[WordNet](http://wordnet.princeton.edu/wordnet)'s 
[synsets](https://en.wikipedia.org/wiki/WordNet#Database_contents). For a given word, 
this implementation does a weighted average of the sentiment scores of each of the 
word's synsets. This means more weight will be given to the scores of the more 
likely *word sense* in general rather than in the current context.

## Configuration

![](%%config procedure import.sentiwordnet)

The `dataFileUri` parameter should point to a SentiWordNet 3.0 data file.
It can be obtained on the [SentiWordNet website](http://sentiwordnet.isti.cnr.it).

## Data format

The row names will be a word followed by a `#` and a one character code indicating the synset type.

The following table shows the synset codes ([source](http://wordnet.princeton.edu/wordnet/man/wndb.5WN.html#sect3)):

| *Code* | *Name* |
|--------|--------|
| n |  NOUN |
| v | VERB |
| a | ADJECTIVE |
| s | ADJECTIVE SATELLITE |
| r | ADVERB |

Assuming the SentiWordNet data is imported in the *sentiWordNet* table, the following query
gets the embedding for the word *love* in the context of a *verb* and *dog* in the context of 
a *noun*.

```
SELECT * FROM sentiWordNet WHERE rowName() IN ('love#v', 'dog#n')

[
   {
      "columns" : [
         [ "SentiPos", 0, "1970-01-01T00:00:00.000Z" ],
         [ "SentiNeg", 0.1928374618291855, "1970-01-01T00:00:00.000Z" ],
         [ "SentiObj", 0.8071626424789429, "1970-01-01T00:00:00.000Z" ],
         [ "POS", "n", "1970-01-01T00:00:00.000Z" ],
         [ "baseWord", "dog", "1970-01-01T00:00:00.000Z" ]
      ],
      "rowHash" : "7dad6626da208a04",
      "rowName" : "dog#n"
   },
   {
      "columns" : [
         [ "SentiPos", 0.6249999403953552, "1970-01-01T00:00:00.000Z" ],
         [ "SentiNeg", 0.01499999966472387, "1970-01-01T00:00:00.000Z" ],
         [ "SentiObj", 0.3600000143051147, "1970-01-01T00:00:00.000Z" ],
         [ "POS", "v", "1970-01-01T00:00:00.000Z" ],
         [ "baseWord", "love", "1970-01-01T00:00:00.000Z" ]
      ],
      "rowHash" : "80bc820285c4e9a2",
      "rowName" : "love#v"
   }
]
```

# See also

* The ![](%%doclink pooling function) is used to embed a bag of words in a vector space like SentiWordNet
* The [Word2Vec tool](https://code.google.com/p/word2vec/) project page
  contains source code to train your own embeddings.
* [SentiWordNet lexical resource](http://sentiwordnet.isti.cnr.it) home page where
  you can find publications as well the data file.
* [WordNet](http://wordnet.princeton.edu/wordnet): a lexical database of English

