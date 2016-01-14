# TF-IDF Function

The TF-IDF function is used to find how relevant certain words are to a document, by 
combining the term frequency (TF), i.e how frequent the term is in the document, with the inverse document frequency (IDF), i.e how frequent a term
appears in a reference corpus.

## Configuration
![](%%config function tfidf)

### Type of TF scoring

Given a term \\( t \\), \\( D \\) the corpus, a document \\( d \in D \\) and the term frequency denoted by \\(f_{t,d}\\), 
the type of TF weighting schemes are:

| *weighting scheme* | *description* | *TF weight* |
|--------------------|---------------|-------------|
| `raw` | the term frequency in the document | \\(f_{t,d}\\) |
| `log` | the logarithm of the term frequency in the document | \\( \log(1 + f_{t,d}) \\) |
| `augmented` | the half term frequency in the document divided by the maximum frequency of any term in the document | \\( 0.5 + 0.5 \cdot \frac { f_{t,d} }{\max_{\{t' \in d\}} {f_{t',d}}} \\) |

### Type of IDF scoring

Given:

- \\( N \\): the total number of documents in the corpus (\\( N = |D| \\))
- \\( n_t = |\{d \in D: t \in d\}| \\): number of documents where the term \\( t \\) appears in

The types of IDF weighting schemes are:

| *weighting scheme* | *description* | *IDF weight*  |
|--------------------|---------------|--------------|
| `unary` | unary IDF score, i.e. don't use IDF | \\( 1 \\) |
| `inverse` | the logarithm of the number of documents in the corpus divided by the number of documents the term appears in (this will lead to negative scores for terms appearing in all documents in the corpus) | \\( \log \left( \frac {N}{1 + n_t } \right) \\) |
| `inverseSmooth` | similar to `inverse` but with 1 added to the logarithmic term (this ensures that the score will never be negative) | \\( \log \left( 1 + \frac {N}{1 + n_t } \right) \\) |
| `inverseMax` | similar to `inverseSmooth` but using the maximum term frequency | \\( \log \left(1 + \frac {\max_{\{t' \in d\}} n_{t'}} {1 + n_t}\right)  \\)
| `probabilisticInverse` | similar to `inverse` but substracting the number of documents the term appears in from the total number of documents in the training corpus (this can lead to positive and negative scores) | \\(  \log \left( \frac {N - n_t} {1 + n_t} \right) \\)


## Input and Output Values

The function takes a single input named `input` that contains the list of words to be evaluated, with the column name being the term, and the value being the number of time the word is present in the document. This can be prepared using the tokenize function or any other method.

The function returns a single output named `output` that contains the combined TF-IDF score for each term in the input.

## See also
* The ![](%%doclink tfidf.train procedure) trains the data to use in this function.
- [tf-idf on Wikipedia](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)

