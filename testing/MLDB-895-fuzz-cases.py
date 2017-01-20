#
# MLDB-895-fuzz-cases.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


def assert_fail(qry):
    try:
        mldb.get('/v1/query', q=qry)
    except mldb_wrapper.ResponseException as exc:
        result = exc.response
    else:
        assert False, 'should not be here'
    mldb.log(result.text)

# problem was a modulus by 0
assert_fail('select CAST (61971-71%0 AS timestamp)')
assert_fail('select CAST (61971-7%-0 AS timestamp)')

# this was a bug of precedence - operator % has higher precedence than IN
# so this expression evaluates to (1%2) IN (4) = 1 IN (4) = false
# and NOT 1 % (2 IN (4)) = 1 % false = 1 % 0
mldb.log(mldb.get('/v1/query', q='select 1 % 2IN (4)'))

# the double dash is starting a comment
# this is equivalent to select 01
mldb.log(mldb.get('/v1/query', q='select 01--1%0e83^ 2 + (9)'))

# this was a bug of precedence - this expression is evaluated as
# 2 IN (-(1%1)-11, 1 IN (21%1,11), (1%1)%1 IN (31%1,11), (1%1)<(1%1), 11)
# = 2 IN (-12, True, True, False, 11)
# = 2 IN (-12, 1, 1, 0, 11)
# = False
mldb.log(
    mldb.get('/v1/query', 
             q='select 2IN (-1%1-11,1IN (21%1,11),1%1%1IN (31%1,11),1%1<1%1,11)'))

# problem with unary bitwise operator print()
assert_fail('select {~9as*}')

mldb.script.set_return("success")
