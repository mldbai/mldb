# Filter Stop Words Function

Functions of this type take a row as an argument and return that same row minus any columns whose names appear on the configured stopword list (case-insensitively). This list is currently the MySQL stopword list.

## Configuration

![](%%config function filter_stopwords)

This function currently has no configuration options.

## Input and Output Values

Functions of this type have a single input value named `words` which is a row, and 
a single output value also named `words`.
