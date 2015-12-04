# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

import json
import random
import math
import sys
import operator

def parse_response(res):
    return json.loads(res['response'])

def check_res(res, value):
    if res['statusCode'] != value:
        mldb.log(parse_response(res))
        assert False, "response status code is %s but expected code %s" % (res["statusCode"], value)
    return parse_response(res)

ds1 = mldb.create_dataset({
    'type': 'sparse.mutable',
    'id': 'dummy'})

ds1.record_row('dummy_row', [['dummy', random.random(), 0]])

ds1.commit()

# mldb should be as precise as python
no_overflow_op_of_1_float = {
    'ceil':math.ceil,
    'floor':math.floor,
    'abs':math.fabs,
    'sqrt':math.sqrt,
    'ln':math.log
}

overflow_op_of_1_float = {
    'exp':math.exp
}

overflow_op_of_2_float = {
    'pow':math.pow
}

# unlike C, python returns moduli, that is the remainders, with the sign of the divisor y
def c_style_mod(x,y):
    mod = x % y
    return mod if math.copysign(1,x) == math.copysign(1,y) or mod == 0 else mod - y

no_overflow_op_of_2_int = {
    'mod':c_style_mod
}


def test_op(sql_op_name, op, float1, float2 = None):
 
    if float2:
        equality = '{0} = {3}({1}, {2})'.format(repr(op(float1, float2)), repr(float1), repr(float2), sql_op_name)
    else:
        equality = '{0} = {2}({1})'.format(repr(op(float1)), repr(float1), sql_op_name)

    mldb.log('testing {}'.format(equality))
    res = mldb.perform('GET', '/v1/query',
                       [['q', 'SELECT {0} AS result FROM dummy'.format(equality)]])
        
    response = check_res(res, 200)

    # if the equality above does not hold true
    if not response[0]['columns'][0][1]:
        assert False, 'there is lost of precision with this query [{}]'.format(equality)
             
def test_ops(ops, float1, float2 = None):

    for sql_op_name, op in ops.iteritems():
        test_op(sql_op_name, op, float1, float2)


def test_with_random_values(iteration):
    for i in range(iteration):
        large_positive_float = random.uniform(0, sys.float_info.max)
        small_positive_float = random.uniform(0, sys.float_info.min)
        large_positive_int = random.randint(0, sys.maxint)
        large_negative_int = random.randint(-sys.maxint - 1, 0)

        test_ops(no_overflow_op_of_1_float, large_positive_float)
        test_ops(no_overflow_op_of_2_int, large_positive_int, 1534)
        test_ops(no_overflow_op_of_2_int, large_negative_int, 1534)
        test_ops(no_overflow_op_of_1_float, small_positive_float)
        test_ops(overflow_op_of_1_float, small_positive_float)

test_with_random_values(100)
# special cases I've seen failing because python and C behave differently
test_op('mod', c_style_mod, -2507820791313827470, 1534)
test_op('mod', c_style_mod, -2507820791313827470, -1534)
test_op('mod', c_style_mod, 2507820791313827470, -1534)

mldb.script.set_return('success')
