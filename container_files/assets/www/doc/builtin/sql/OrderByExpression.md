# SQL Order-By Expressions


An SQL Order-By expression is a comma-delimited list of [Value Expressions](ValueExpression.md), where each expression can be followed by `ASC` or `DESC` to specify ascending or descending order, respectively. Query results will be sorted by the order of the expressions in the Order-By expression.

All Order-By expressions (including the default, empty, Order-By expression) have `rowHash()` appended to them, so as to provide a stable sort order at all times.

The Order-By expressions support ordering with complex types (e.g. row) using lexicographical ordering.
Consider this fictive dataset `docs` containing the list of programming languages mentioned in a given set of documents:

| rowName | terms |
| ---- | ------ |
| doc1 | "c++,python,c++,java,c++" |
| doc2 | "scala,scala,java,java,scala,java,scala,c++" |
| doc3 | "python,ada,ada" |

The rows returned by this query

```
SELECT tokenize(terms) as term FROM docs ORDER BY tokenize(terms) 
```

are lexicographically ordered using the ordered column's names (i.e. the programming language name) and the column's values.

```
[
  {
    "columns":[
      ["term.ada",2,"2015-12-01T13:25:54Z"],
      ["term.python",1,"2015-12-01T13:25:54Z"]
    ],
    "rowHash":"47adff9d728370ae",
    "rowName":"doc3"
  },
  {
    "columns":[
      ["term.c++",1,"2015-12-01T13:25:54Z"],
      ["term.java",3,"2015-12-01T13:25:54Z"],
      ["term.scala",4,"2015-12-01T13:25:54Z"]
    ],
    "rowHash":"a5b0ac997090aa3e",
    "rowName":"doc2"
  },
  {
    "columns":[
      ["term.java",1,"2015-12-01T13:25:54Z"],
      ["term.python",1,"2015-12-01T13:25:54Z"],
      ["term.c++",3,"2015-12-01T13:25:54Z"]
    ],
    "rowHash":"86065feec3521acc",
    "rowName":"doc1"
  }
]
```

More details about the ordering of complex types can be found in [MLDB Type System](TypeSystem.md).

Should the Order-By expression return values of varying types, in ascending order they will be sorted in this fashion: First `null` values, then atomic values, then row values, and finally embedding values.
