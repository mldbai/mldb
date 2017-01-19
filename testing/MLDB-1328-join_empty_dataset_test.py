#
# MLDB-1328-join_empty_dataset_test.py
# Mich, 2016-01-28
# Copyright (c) 2016 mldb.ai inc. All rights reserved.
#
# Tools for the plugins and their tests.
import unittest

if False:
    mldb = None


def log(thing):
    mldb.log(str(thing))


def perform(*args, **kwargs):
    res = mldb.perform(*args, **kwargs)
    assert res['statusCode'] in [200, 201], str(res)
    return res


class JoinEmptyDatasetTest(unittest.TestCase):

    def test_it(self):
        perform('PUT', '/v1/datasets/ds', [], {
            'type' : 'sparse.mutable'
        })
        perform('POST', '/v1/datasets/ds/commit', [], {})
        qry = "SELECT uid, count(1) AS size FROM ds GROUP BY uid"
        res = perform('GET', '/v1/query', [['q', qry]])
        assert res['response'] == '[]'

if __name__ == '__main__':
    if mldb.script.args:
        assert type(mldb.script.args) is list
        argv = ['python'] + mldb.script.args
    else:
        argv = None

    res = unittest.main(exit=False, argv=argv).result
    log(res)
    got_err = False
    for err in res.errors + res.failures:
        got_err = True
        log(str(err[0]) + "\n" + err[1])

    if not got_err:
        mldb.script.set_return("success")
