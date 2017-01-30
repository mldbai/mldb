#
# MLDB-832-select_star.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

"""
This test checks that different select statements return the right columns
"""

def check_res(res, value):
    assert res.status_code == value, res.text
    return res.json()

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'd1'})

ds1.record_row('row_0', [['a', 0, 0],
                         ['b', 1, 0],
                         ['c', 2, 0],
                         ['x1', 3, 0],
                         ['x2', 4, 0],
                         ['x3', 5, 0]])
ds1.commit()

def run_query(select, expected):
    """ run a query using `select` on and compare the (sorted) column names
        with the `expected` column names
        ex:
            select = 'a, c, b'
            expected = 'a b c'
    """
    expected = expected.split()
    out = mldb.query('SELECT {} FROM {}'.format(select, 'd1'))
    cols = sorted(out[0][1:])
    if cols != expected:
        mldb.log('{} != {}'.format(cols, expected))
        mldb.log('output was')
        mldb.log(out)
        assert False
    return out

# simple queries like in the doc
run_query('*',
          'a b c x1 x2 x3')
run_query('{*} as *',
          'a b c x1 x2 x3')
run_query('{*\n} as *',
          'a b c x1 x2 x3')
run_query('{{*} as *} as *',
          'a b c x1 x2 x3')
# following test case shows the bug from MLDB-1205
run_query('{{*} as *\n} as *',
          'a b c x1 x2 x3')
run_query('a,b,c,x1',
          'a b c x1')
run_query('* EXCLUDING (a)',
          'b c x1 x2 x3')
run_query('* EXCLUDING (a,c)',
          'b x1 x2 x3')
run_query('* EXCLUDING (x*)',
          'a b c')
run_query('* EXCLUDING (a, x*)',
          'b c')
run_query('x* AS y*',
          'y1 y2 y3')
run_query('x* EXCLUDING(x3) AS y*',
          'y1 y2')
# not sure that's how this one should behave
run_query('a, a', 'a')

# simple using object(select ...)
run_query('{*} as z',
          'z.a z.b z.c z.x1 z.x2 z.x3')

run_query('{a,b} as z',
          'z.a z.b')
run_query('{x*} as z',
          'z.x1 z.x2 z.x3')
run_query('a,b,c, {x*} as z',
          'a b c z.x1 z.x2 z.x3')

# Now a few with functions
conf = {'type': 'sql.expression',
    'params': {
        'expression': '{x*} as z'
    }}
res = mldb.put('/v1/functions/xs_as_z', conf)
check_res(res, 201)

run_query('a,b,c, xs_as_z({x*}) as *',
          'a b c z.x1 z.x2 z.x3')
run_query('xs_as_z({*}) as *',
          'z.x1 z.x2 z.x3')
run_query('xs_as_z({*})[z] as *',
          'x1 x2 x3')
run_query('xs_as_z({*})[z] as z',
          'z.x1 z.x2 z.x3')
#run_query('xs_as_z({*})[z as *] as *',
#          'x1 x2 x3')
run_query('xs_as_z({*})[{z.x1, z.x2}] as *',
          'z.x1 z.x2')

run_query('xs_as_z({*}) as *', 'z.x1 z.x2 z.x3')

# here I'm "calling" the function twice in another function
res = mldb.put('/v1/functions/twice', {
    'type' : 'sql.expression',
    'params': {
        'expression': 'xs_as_z({x*})[z] as w,'
                      'xs_as_z({x*})[z] as z'
    }})
check_res(res, 201)

run_query('twice({*}) as *',
          'w.x1 w.x2 w.x3 z.x1 z.x2 z.x3')

# same thing but once
res = mldb.put('/v1/functions/once', {
    'type' : 'sql.expression',
    'params': {
        'expression': 'xs_as_z({x*})[z] as w,'
    }})
check_res(res, 201)

run_query('once({*}) as *', 'w.x1 w.x2 w.x3')

mldb.script.set_return('success')
