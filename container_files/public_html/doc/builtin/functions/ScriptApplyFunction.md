# Script Apply Function

The script apply function type is used to create functions written in Javascript or Python script.

## Configuration

![](%%config function script.apply)

With `ScriptResource` defined as:

![](%%type Datacratic::MLDB::ScriptResource)

## Input and Output Values

Functions of this type have a single input value named `args`, that represents a row. The contents will be available to the script in the args parameter.

These functions output a single value called `return`. It must be an array of arrays, where the inside array is a 3 element tuple `[column_name, value, ts]`.


## Example

Assume a dataset called `myData` with the following contents:

|  rowName   |  x  |  y  |
|----------|---|---|
| row1     | 1 | 4 |
| row2     | 2 | 2 |

To create a Python function called `myFunction` that will take each column an multiply it by two, we can use the following script:

```python
results = []
for colName, cell in mldb.script.args[0]:
    cellValue, cellTs = cell
    results.append([colName, cellValue*2, cellTs])

mldb.script.set_return(results)
```

You can now apply `myFunction` on the `myData` dataset with the following query:

```sql
select myFunction({ {*} as args })[return] from myData
```

The `return` value will contain:

```python
[
   {
      "columns" : [
         [ "x", 2, "2015-07-17T11:49:46.000Z" ],
         [ "y", 8, "2015-07-17T11:49:46.000Z" ]
      ],
      "rowHash" : "1c710c9517693187",
      "rowName" : "row1"
   },
   {
      "columns" : [
         [ "x", 4, "2015-07-17T11:49:46.000Z" ],
         [ "y", 4, "2015-07-17T11:49:46.000Z" ]
      ],
      "rowHash" : "905abb69bcf24660",
      "rowName" : "row2"
   }
]
```

## See also

* the ![](%%doclink python plugin) Server-Side API
* the ![](%%doclink javascript plugin) Server-Side API

