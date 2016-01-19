# JSON Import Procedure

The JSON Import Procedure type is used to import a text file containing
one JSON per line in a dataset.


## Configuration

![](%%config procedure import.json)

## Example of data recording

The following algorithm is used to record data:

Each `(key, value)` pair will be recorded as the column name and cell value respectively.

The line:

    {"a": 5, "b": true}

is recorded as:

| *rowName* | *a* | *b* |
|-----------|-----|-----|
| row1 | 5 | true |

If the value is an object, we apply the same logic recursively, adding an underscore
between the keys at each level.

The line:

    {"a": 5, "b": true, "c": {"x": "hola"}}

is recorded as:

| *rowName* | *a* | *b* | *c_x* |
|-----------|-----|-----|-------|
| row1 | 5 | true | hola |


If the value is an array that contains only atomic types (strings, bool or numeric), we
encode them as one hot vectors.


The line:

    {"a": 5, "b": [1, 2, "abc"]}

is recorded as:

| *rowName* | *a* | *b_1* | *b_2* | *b_abc* |
|-----------|-----|-----|-------|-----------|
| row1 | 5 | true | true | true |


If the value is an array that contains at least one non-atomic type (array, object), we
encode them as the string representation of the JSON.

The line:

    {"a": 5, "b": [1, 2, {"xyz":"abc"}]}

is recorded as:

| *rowName* | *a* | *b* |
|-----------|-----|-----|
| row1 | 5 | [1, 2, {"xyz":"abc"}] |

## See also

* The ![](%%doclink text.csv.tabular dataset) can be used to import a CSV file

