#
# MLDB-1165-where-rowname-in-optim.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#
import datetime

mldb = mldb_wrapper.wrap(mldb) # noqa

dataset = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'example_large'
})

for i in xrange(20000):
    dataset.record_row("u%d" % i, [['x', "whatever", 0]])

dataset.commit();

expected = [["_rowName","x"], ["u1","whatever"],["u12","whatever"],["u123","whatever"],["u1234","whatever"],["u12345","whatever"]];

now = datetime.datetime.now()

result = mldb.query(
    "select * from example_large WHERE rowName() IN "
    "('u1', 'u12', 'u123', 'u1234', 'u12345', 'u123456')")

delta = datetime.datetime.now() - now;

mldb.log(delta.seconds)
mldb.log(delta.microseconds)

# disabled so as not to cause spurious failures
# assert delta.microseconds < 15000 # should take ~1k us with optim, +20k without

assert result == expected

#MLDB-1615

dataset = mldb.create_dataset({
        'type': 'sparse.mutable',
        'id': 'example_small'
})

for i in xrange(10):
    dataset.record_row("u%d" % i, [['x', "whatever", 0]])

dataset.commit();

result = mldb.query(
    "select * from example_small WHERE rowName() NOT IN "
    "('u1', 'u3', 'u5', 'u7') order by rowPath()")

expected = [["_rowName","x"],["u0","whatever"],["u2","whatever"],["u4","whatever"],["u6","whatever"],["u8","whatever"],["u9","whatever"]]

mldb.log("result");
mldb.log(result)
mldb.log("expected");
mldb.log(expected)

assert result == expected

mldb.script.set_return("success")
