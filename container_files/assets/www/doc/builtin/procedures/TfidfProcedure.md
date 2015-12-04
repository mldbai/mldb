TF-IDF Procedure

The TF-IDF procedure trains the data to use a TF-IDF function. This function is used to find how relevant certain words are to a document, by 
combining the term frequency (TF), i.e how frequent the term is in the document, with the inverse document frequency (IDF), i.e how frequent a term
appears in a reference corpus.

## Configuration
![](%%config procedure tfidf.train)

## Input and Output Values

In the input dataset of the procedure, each row is a document and each column is a term, with the value being something other than 0 if the term appears in
the document. This can be prepared using the tokenize function or any other method.

In the output dataset, a single row is added, with the columns being each term present in the corpus, and the value being the number of document the term appears in.