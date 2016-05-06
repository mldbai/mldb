# Singular Value Decomposition Training Procedure

This procedure allows for a truncated, abbreviated singular value decomposition
to be trained over a dataset.This procedure
trains an SVD model and stores the model file to disk and/or applies the model to 
the input data to produce an embedded output dataset for columns and/or rows.

## Algorithm description

A singular value decomposition is a way of more compactly representing a dataset,
by taking advantage of the known relationships and redundancy between the different
columns in the dataset.  It is most often used to create an unsupervised embedding
of a dataset.  Mathematically, it looks like this:

$$
A \approx U \Delta V^T
$$

Where A is the input matrix (dataset), U is an orthonormal matrix known as the
left-singular vectors, V is an orthonormal matrix known as the right-singular
vectors, and \\(\Delta\\) is a diagonal matrix with the entries on its diagonal
known as the Singular Values.

Intuitively, \\(U\\) tells us about how to represent a row as a coordinate in a
vector space, and \\(V\\) tells us how to represent a column as a coordinate in
the same vector space.  \\( \Delta \\) tells us how important each of those coordinates
is in the reconstruction.

As input, it takes a single dataset \\(A\\).  The algorithm is designed for
the case where the input is tall and narrow, in other words where the
number of rows is higher or much higher than the number of columns.  It can
work reasonably well up to hundreds of millions of rows and a few million
columns.

### Preprocessing

The SVD algorithm is designed to be robust against real-world datasets, and so
preprocessing is done on the columns before the algorithm is applied.  Each
input column is converted into one or more virtual columns as follows:

* Real valued, dense columns are used as-is.  Note that infinite values are not
  currently accepted.  Not a Number ("NaN") values are assumed to be missing.
* String valued columns are converted into multiple columns, with one column
  per string value.  These sparse columns have the value `1` where the string has
  the given value, and are missing else where.
* Mixed-value columns are currently not accepted.

Columns that have multiple values for the same row have undefined results:
they may use an average, or take an arbitrary one of the values, etc.

The following preprocessing is not performed, and should be done manually if
needed:

1.  Strings values are not converted to bag-of-words representations or anything
    like that.  That preprocessing needs to be performed manually.
2.  In the case of categorical columns encoded as integers, the algorithm will
    treat it as a real value.  For categorical columns, it's best to use strings,
    even if the string just encodes the number (`'1'` versus `1`).

### Truncation

The SVD computed is a *truncated* SVD.  This means that it calculates a limited set
of singular vectors and singular vectors that represent the input matrix as closely
as possible.  Typically between a few tens and a thousand or so singular values will
be used.

### Abbreviation

The SVD is computed on an abbreviated subspace of a representitive sample of
columns from the initial column space.  Currently, the `numDenseBasisVectors` least
sparse columns are included in the subspace.  This parameter can be used to
control the runtime of the algorithm when there are lots of sparse columns.

The embeddings of all columns are calculated, even if they are not one of the
dense basis vectors.

## Format of the output

The SVD algorithm produces three outputs:

* A SVD model file, which can be loaded by the ![](%%doclink svd.embedRow function).
  This is a JSON formatted file, and so can be inspected or imported into another
  system.  It contains a `columnIndex` map showing how to map sparse column
  values onto expanded columns, a `columns` array with the singular vector
  and scaling information for each column, and a `singularValues` array giving
  the singular values of each columns.
* A dataset (if the `rowOutput` section is filled in) containing the singular
  vectors for each row in the input dataset.  This will be a dense matrix
  with the same number of
  rows as the input dataset, and columns with names prefixed with the
  `outputColumn` and a 4 digit number for each of the singular values.
* A dataset (if the `output` section is filled in) containing the singular
  vectors for each column in the input dataset.  This will be a dense matrix
  with a row for each of the virtual columns created as part of the preprocessing,
  and the same columns as the `rowOutput` dataset.  Note that this dataset is
  transposed with respect to the input dataset.
  
  The names of the columns in the output depend upon the type of the virtual
  columns:
  - A virtual column for the numeric value of a column has a row name the
    same as the column name;
  - A virtual column for a string column has a row name that is the column
    name plus a `|` plus the string value.


## Configuration

![](%%config procedure svd.train)

## Restrictions

- The SVD algorithm as implemented is designed for the embedding of high dimensional
  datasets into a lower dimensional space.  In the case of rank-constrained input
  datasets (ie, very simple datasets with only a few variables and clear relationships
  between them), it will not necessarily give good results.  For those, it may be
  better to skip the embedding step altogether.
- Columns with a mixture of strings and numbers are currently not accepted.  This
  will be rectified in a future release.
- Columns that contain infinite values are currently not accepted.  This will be
  rectified in a future release.

## Troubleshooting

If the SVD produces all-zero vectors for rows or columns, it may be one of the following:

- A column which is present in only a single row with no other columns will have a zero embedding vector
- A row which is present only in a single column with no other rows will have a zero embedding vector
- If all of the values in the columns for a given row are zero, the row will have a zero embedding vector
- If all of the values in the rows for a given column are zero, the column will have a zero embedding vector
- An empty row or empty column will have a zero embedding vector

(For an SVD to produce meaningful results, it needs to be able to determine how a given
column varies with other columns, which means it needs to be present in two or more
rows, each of which contain columns present in two or more rows.  Same holds in the
other direction).

## Examples

* The ![](%%nblink _demos/Recommending Movies) demo notebook
* The ![](%%nblink _demos/Mapping Reddit) demo notebook
* The ![](%%nblink _demos/Visualizing StackOverflow Tags) demo notebook


## See also

* [Wikipedia SVD Article](http://en.wikipedia.org/wiki/Singular_value_decomposition)
* ![](%%doclink svd.embedRow function)
* [REST API] (../rest/Types.md)

