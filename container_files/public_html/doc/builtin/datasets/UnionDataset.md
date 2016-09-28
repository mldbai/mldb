# Union Dataset

The union dataset allows for rows from multiple datasets to be appended
into a single dataset. Columns that match up between the datasets will be
combined together. Row names are altered to reflect the dataset they came
from and avoid having them merged together.

For example, the row names of to unified datasets will have the following
format.

```
0.dataset index 0 row name
...
n.dataset index n row name
```

The union is done on the fly which means it is relatively rapid to unify even
large datasets together.

Aside from the resulting row names, creating a union dataset is equivalent to
the following SQL:

```sql
SELECT s1.* AS *, s2.* AS *
FROM (SELECT * FROM ds1 ) AS s1
OUTER JOIN (SELECT * FROM ds2) AS s2 ON false
```

## Configuration

![](%%config dataset union)
