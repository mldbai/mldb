# Pooling Function

The Pooling Function type creates a pooling function to embed documents into a word space
by using aggregators to combine the word embeddings. Conceptually, provided we have a 
representation of words, the function provides a way to represent a document by the 
combination of all the words it contains.

## Configuration

![](%%config function pooling)

The `aggregators` specifies the type of pooling that will be performed. To do average
pooling, use the `avg` aggregator, etc.

## Input and Output Values

Functions of this type have a single input value named `words` which is a row, and 
a single output value named `embedding`.

## Example

Suppose we have a word embedding represented by the following dataset called `word_embedding`:

| rowName | x | y | z |
|-----------|-----|-----|-----|
| hello | 0.2 | 0.95 | 0.4 |
| friend | 0.8 | 0.01 | 0.5 |
| best | 0.4 | 0.5 | 0.6 |

Also suppose we have the following bag of words in the `bag_of_words` dataset. 
This can be obtained by applying the `tokenize` function to any text field:

| rowName | hello | friend | best | my |
|-----------|---------|----------|--------|------|
| doc1 | 1 | 1 |
| doc2 | 1 | 1 | 1| 1 |

Let's configure a pooling function:

```javascript
PUT /v1/functions/pooler
{
    "type": "pooling",
    "params": {
        "aggregators": ["avg"],
        "embeddingDataset": "word_embedding"
    }
}
```

We can now do the following call:


```sql
SELECT pooler({words: {*}})[embedding] as embed from bag_of_words
```

| rowName | embed.000000 | embed.000001 | embed.000002 |
|-----------|---------|----------|--------|
| doc1 | 0.5 | 0.48 | 0.45 |
| doc2 | 0.466 | 0.516 | 0.5 | 


## See also

### MLDB Embedding importers
* The ![](%%doclink import.word2vec procedure) imports a Word2Vec embedding.
* The ![](%%doclink import.sentiwordnet procedure) imports a SentiWordNet embedding.

