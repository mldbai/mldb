#
# MLDB-797-nested_sql_expressions.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
mldb = mldb_wrapper.wrap(mldb) # noqa

def check_res(res, code):
    assert res.status_code == code, res.text

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'ds1'})


ds1.record_row('row_0', [['x', 1, 0], ['y', 2, 0]])
ds1.record_row('row_1', [['x', 1, 0], ['y', 3, 0]])
ds1.record_row('row_2', [['y', 4, 0]])
ds1.commit()

# first void sql.expression
res = mldb.put('/v1/functions/patate', {
    'type': 'sql.expression',
    'params': {
        'expression': '*'}})
check_res(res, 201)

# second void sql.expression that uses the first one
res = mldb.put('/v1/functions/poil', {
    'type': 'sql.expression',
    'params': {
        'expression': 'patate({*})'}})
check_res(res, 201)

# query calling through both
res = mldb.get('/v1/query', q='SELECT poil({*}) from ds1')
check_res(res, 200)


mldb.script.set_return('success')
