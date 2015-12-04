# Intro to Datasets

Datasets are named sets of data points composed of (row, column, timestamp, value) tuples. Datasets can have millions of named columns and rows, and data points can take on multiple timestamped values which can be numbers or strings. 

Datasets can be loaded from or saved to files, and can serve as the input to Procedures as well as be created by them. And of course, Datasets can be queried via [SQL queries](../sql/Sql.md). 

## Matrix View

The following diagram shows this as a 3-dimensional matrix, and shows what
happens when we "slice" the matrix at a particular point in time.

![Sliced Dataset](/doc/builtin/img/SlicedDataset.svg)

Ignoring the time dimension, you can imagine that the data looks something like this:

<table cellpadding="0" cellspacing="0" class="c17"><tbody><tr class="c3"><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c7"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c7">ColumnA</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c7">ColumnB</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c7">ColumnC</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c7">ColumnD</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c7">ColumnE</span></p></td></tr><tr class="c3"><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c7">RowA</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td></tr><tr class="c3"><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c7">RowB</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c0">123</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c0">7634.2</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td></tr><tr class="c3"><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c7">RowC</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c0">&quot;hello&quot;</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td></tr><tr class="c3"><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c7">RowD</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td></tr><tr class="c3"><td class="c5" colspan="1" rowspan="1"><p class="c8"><span class="c7">RowE</span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td><td class="c5" colspan="1" rowspan="1"><p class="c8 c12"><span class="c0"></span></p></td></tr></tbody></table>

There are several important points here:

1. Each "cell" has a row name, a column name, and a value.
1. The values can be strings, integers, floating point numbers, or empty
1. The row names and column names are NOT numbers.  They are arbitrary strings. 
1. There is no concept of rows or columns being "in order" like a spreadsheet.

We also allow each column and row to have a different value at each time point.

## Events View

Another way of looking at the data is as a series of events, as follows:

<table cellpadding="0" cellspacing="0" class="c17"><tbody><tr class="c3"><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c7">Timestamp</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c7">Row Name</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c7">Column Name</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c7">Value</span></p></td></tr><tr class="c3"><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">2013-04-20 10:02:01</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">User123</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">First Name</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">&quot;Bob&quot;</span></p></td></tr><tr class="c3"><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">2013-04-20 10:02:01</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">User123</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">Test Score 1</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">0.78</span></p></td></tr><tr class="c3"><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">2013-04-20 10:03:33</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">User456</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">First Name</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">&quot;Jill&quot;</span></p></td></tr><tr class="c3"><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">2013-04-20 10:03:33</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">User456</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">Test Score 1</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">0.45</span></p></td></tr><tr class="c3"><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">2013-04-22 11:10:22</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">User123</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">Test Score 1</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">0.0</span></p></td></tr><tr class="c3"><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">2013-04-22 11:10:22</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">User123</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">Revision Reason</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">&quot;Cheating&quot;</span></p></td></tr><tr class="c3"><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">...</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">...</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">...</span></p></td><td class="c4" colspan="1" rowspan="1"><p class="c8"><span class="c0">...</span></p></td></tr></tbody></table>

The ability to record multiple states of the world at different times
is a fundamental part of the design of MLDB as a database for machine
learning.  The two versions of
Bob's score are both recorded, along with the time at which it changed.
This allows MLDB to reconstruct what was known at a given point in time,
making historical data far more useful to learn from.

## Rows Versus Columns

Under the MLDB data model, rows and columns are isomorphic.  In other words,
they are both represented in the same way.

That being said, most algorithms work best when the number of rows is greater
than the number of columns.  So if you are using rows to represent users
(of which there are lots) and columns to represent attributes (of which there
are fewer), then the algorithms will run better with the data represented this
way than the inverse.

In the rare cases where this doesn't work, it is possible to transpose a dataset with the ![](%%doclink transposed dataset),
so that rows become columns and vice-versa, but this can be highly inefficient,
and most well designed algorithms do not require this to be done.


## Available Dataset Types

Datasets are created via a [REST API call](DatasetConfig.md) with one of the following types:

![](%%availabletypes dataset table)


## See also

- [Query API reference] (../sql/QueryAPI.md)
* ![](%%nblink _tutorials/Loading Data Tutorial) 
* ![](%%nblink _tutorials/Querying Data Tutorial) 
