# Melt Procedure

This procedure type allows you to perform a melt operation on a dataset. Melting 
a dataset takes a set of columns to keep fixed and a set of columns to melt and
creates a new rows, one per column to melt, while copying as is the columns to 
keep fixed.

An example usage is importing JSON data where some fields contain an array of 
objects and the way we want to process it is one object per row.

## Configuration

![](%%config procedure melt)

## Example

Suppose the following dataset `data` with the `friends` column containing strings.

| *rowName* | *name* | *age* | *friends* |
|-----------|--------|-------|-----------|
| row1 | bill | 25 | [{"name": "mich", "age": 20}, {"name": "jean", "age": 18}] |

We may want to perform operations on the contents of the JSON object in the 
`friends` column. To do so, we can perform a `melt` operation on the output
of the `parse_json()` function.

Doing the query `select parse_json(friends, {arrays: 'encode'}) from data` will return:

| *rowName* | *friends.0* | *friends.1* |
|-----------|--------|-------|-----------|
| row1 | {"name": "mich", "age": 20} | {"name": "jean", "age": 18} |

We can do the melt like this:

    PUT /v1/procedures/melt
    {
        "type": "melt",
        "params": {
            "trainingData": """
                        SELECT {name, age} as to_fix,
                               {friends*} as to_melt
                        FROM (
                            SELECT parse_json(friends, {arrays: 'encode'}) AS * from data
                        )""",
            "outputDataset": "melted_data"
            "runOnCreation": True
        }
    }

The `melted_data` dataset will look like this:

| *rowName* | *name* | *age* | *key* | *value* |
|-----------|--------|-------|-----------|-----|
| row1_friends.0 | bill | 25 | friends.0 | {"name": "mich", "age": 20} |
| row1_friends.1 | bill | 25 | friends.1 | {"name": "jean", "age": 18} |


## See also

* The [parse_json](../sql/ValueExpression.md.html#parse_json) builtin function can perform 
JSON unpacking to a text cell in an SQL query
