# TF-IDF Function

The TF-IDF function is used to find how relevant certain words are to a document, by 
combining the term frequency (TF), i.e how frequent the term is in the document, with the inverse document frequency (IDF), i.e how frequent a term
appears in a reference corpus.

## Configuration
![](%%config function tfidf)

## Input and Output Values

The function takes a single input named `input` that contains the list of words to be evaluated, with the column name being the term, and the value being the number of time the word is present in the document. This can be prepared using the tokenize function or any other method.

The function returns a single output named `output` that contains the combined TF-IDF score for each term in the input.
