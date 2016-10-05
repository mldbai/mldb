# Obtaining information about types

MLDB includes a built-in type introspection system, which is particularly useful for
building UIs that can be used for multiple algorithms.

## Learning how to congfigure an entity of a given type

In order to obtain the type used to configure an entity of a given type, you can
call the

```python
mldb.get("/v1/types/<entityClass>/<entityType>/info")
```

route, and look at the `configType` parameter.  For example, for the svd.train procedure,
you would call

```python
mldb.get("/v1/types/procedures/svd/info")
```

which returns

```python
{
   "configType" : {
      "fields" : [
         {
            "comment" : "Dataset on which the SVD is trained.",
            "default" : {},
            "name" : "dataset",
            "offset" : 0,
            "type" : {
               "c++TypeName" : "MLDB::PolyConfigT<MLDB::Dataset const>",
               "documentationUri" : "/doc/builtin/datasets/DatasetConfig.md",
               "kind" : "STRUCTURE",
               "typeName" : "Dataset (read-only)"
            }
         },
         {
            "comment" : "Output dataset for embedding (column singular vectors go here)",
            "default" : {},
            "name" : "output",
            "offset" : 64,
            "type" : {
               "c++TypeName" : "MLDB::PolyConfigT<MLDB::Dataset>",
               "documentationUri" : "/doc/builtin/datasets/DatasetConfig.md",
               "kind" : "STRUCTURE",
               "typeName" : "Dataset"
            }
         },
         {
            "comment" : "Output dataset for embedding (row singular vectors go here)",
            "default" : {},
            "name" : "rowOutput",
            "offset" : 128,
            "type" : {
               "c++TypeName" : "MLDB::PolyConfigT<MLDB::Dataset>",
               "documentationUri" : "/doc/builtin/datasets/DatasetConfig.md",
               "kind" : "STRUCTURE",
               "typeName" : "Dataset"
            }
         },
         {
            "comment" : "Uri at which we store the SVD.  Optional.  If empty, the SVD model will not be stored",
            "default" : "",
            "name" : "svdUri",
            "offset" : 192,
            "type" : {
               "kind" : "STRING",
               "typeName" : "std::string"
            }
         },
         {
            "comment" : "Maximum number of singular values to work with.  If there are not enough degrees of freedom in the dataset (it is rank-deficient), then less than this number may be used",
            "default" : 100,
            "name" : "numSingularValues",
            "offset" : 208,
            "type" : {
               "kind" : "INTEGER",
               "typeName" : "int"
            }
         },
         {
            "comment" : "Maximum number of dense basis vectors to use for the SVD.  This parameter gives the number of dimensions into which the project is made.  The runtime goes up with the square of this parameter, in other words 10 times as many is 100 times as long to run.",
            "default" : 2000,
            "name" : "numDenseBasisVectors",
            "offset" : 212,
            "type" : {
               "kind" : "INTEGER",
               "typeName" : "int"
            }
         },
         {
            "comment" : "Base name of the column that will be written by the SVD.  A number will be appended from 0 to numSingularValues.",
            "default" : "svd",
            "name" : "outputColumn",
            "offset" : 200,
            "type" : {
               "kind" : "STRING",
               "typeName" : "std::string"
            }
         },
         {
            "comment" : "Select these columns (default all columns).  Only plain column names may be used; it is not possible to select on an expression (like x + 1)",
            "default" : "*",
            "name" : "select",
            "offset" : 216,
            "type" : {
               "c++TypeName" : "MLDB::SelectExpression",
               "documentationUri" : "/doc/builtin/sql/SelectExpression.md",
               "kind" : "ATOM",
               "typeName" : "SqlSelectExpression"
            }
         },
         {
            "comment" : "Only use rows matching this clause (default all rows)",
            "default" : "true",
            "name" : "where",
            "offset" : 272,
            "type" : {
               "c++TypeName" : "std::shared_ptr<MLDB::RowExpression>",
               "documentationUri" : "/doc/builtin/sql/RowExpression.md",
               "kind" : "ATOM",
               "typeName" : "SqlRowExpression"
            }
         }
      ],
      "kind" : "STRUCTURE",
      "typeName" : "MLDB::SvdConfig"
   }
}
```

The information is returned in the following format:

![](%%type MLDB::ValueDescriptionRepr)

The `kind` field looks like this:

![](%%type MLDB::ValueKind)

Structure fields look like this:

![](%%type MLDB::StructureFieldRepr)

and enumeration fields look like this:

![](%%type MLDB::EnumValueRepr)


## Information about other types

In addition, if you need more information about another type listed in one of those
fields (for example, a nested structure or the values of an enumeration), you can
ask for it directly under the route

```python
mldb.get("/v1/typeInfo", type=<typeName>)
```

which will return similar output to that given above.

