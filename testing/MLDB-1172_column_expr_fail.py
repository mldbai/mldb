#
# MLDB-1172_column_expr_fail.py
# Datacratic, 2015
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#
import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset_config = {
    'type'    : 'sparse.mutable',
    'id'      : 'toy'
}


dataset = mldb.create_dataset(dataset_config)
now = datetime.datetime.now()

dataset.record_row("rowA", [["feat1", 1, now],
                            ["feat2", 1, now],
                            ["feat3", 1, now]])
dataset.record_row("rowB", [["feat1", 1, now],
                            ["feat2", 1, now]]),
dataset.record_row("rowC", [["feat1", 1, now]])

dataset.commit()


res = mldb.get(
    "/v1/query",
    q="select COLUMN EXPR (ORDER BY rowCount() DESC LIMIT 2) from toy")

res = mldb.get(
    "/v1/query",
    q="select COLUMN EXPR (WHERE regex_match(columnName(), 'feat[[:digit:]]') "
      "ORDER BY rowCount() DESC LIMIT 2) from toy")

mldb.script.set_return("success")

