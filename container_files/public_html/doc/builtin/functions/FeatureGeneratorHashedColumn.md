# Hashed Column Feature Generator Function (Experimental)

The hashed column feature generator function is a vectorizer that can be used
on large or variable length rows to produce a smaller fixed length feature vector.

## Configuration

![](%%config function experimental.feature_generator.hashed_column)

## Input and Output Value

Functions of this type have a single input value called `columns` which is a row and
a single output value called `hash` which is a row of size $$2^{\text{numBits}}$$.

## See also

* [Feature hashing Wikipedia article](https://en.wikipedia.org/wiki/Feature_hashing)
