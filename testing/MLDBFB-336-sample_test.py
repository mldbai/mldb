#
# MLDBFB-336-sample_test.py
# 2016-01-01
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

# add this line to testing.mk:
# $(eval $(call mldb_unit_test,MLDBFB-336-sample_test.py,,manual))


import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

# TODO: put your own name here e.g. Mldb999Test
class SampleTest(MldbUnitTest):  

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        ds.record_row("a",[["x", 1, 0]])
        ds.commit()
        
    # TODO: put your own name here e.g. test_something
    def test_select_x_works(self):
        # try something that should work
        # mldb.get asserts the result status_code is >= 200 and < 400
        mldb.get("/v1/query", q="select x from sample")

        # assert the result, all unittest asserts are available and
        # assertTableResultEquals was added to facilitate validating query
        # results
        self.assertTableResultEquals(
            mldb.query("select x from sample"),
            [
                ["_rowName", "x"],
                [       "a",  1 ]
            ]
        )


    # TODO: put your own name here e.g. test_something
    def test_errors(self):
        # test a bad query
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.query("SELECT this will not work")

        # the original response is available via re.exception.response
        self.assertEqual(re.exception.response.status_code, 400)

        # directly test the error message
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                  'must override getAllColumns'):
            mldb.query("SELECT *")

    @unittest.expectedFailure
    # TODO: put your own name here e.g. test_something
    def failing_test(self):
        # test a bad query without catching the exception
        mldb.query("SELECT this will not work")

mldb.run_tests()


