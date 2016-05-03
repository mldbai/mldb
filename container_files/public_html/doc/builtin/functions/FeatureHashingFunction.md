# Feature Hashing Function

The hashed column feature generator function is a vectorizer that can be used
on large or variable length rows, or fixed lenght rows with lots of categorical values,
to produce a smaller fixed length numerical feature vector.

## Configuration

![](%%config function feature_hasher)

### Hashing mode

The `mode` field controls what gets hashed:

- `columns`: This mode should be used if you're hashing a generally sparse
*bag of words*, where *words* are in the columns and the value represents
a count. Only the column names will be hashed.
- `columnsAndValues`: This mode should be used if you're hashing dense data,
where fixed columns can take different values. The contatenation of both the
column name and the cell value will be hashed.

## Input and Output Value

Functions of this type have a single input value called `columns` which is a row and
a single output value called `hash` which is a row of size $$2^{\text{numBits}}$$.

## See also

* [Feature hashing Wikipedia article](https://en.wikipedia.org/wiki/Feature_hashing)

