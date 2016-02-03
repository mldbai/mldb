#
# MLDBFB-999_select_x_fails.py
# 2016-01-01
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

class SelectXFailsTest(MldbUnitTest):

  def test_select_x_fails(self):
     # create a dummy dataset
     ds = mldb.create_dataset({
        "id": "sample",
        "type": "beh.mutable", 
        "params":{"dataFileUrl": "file://tmp/MLDBFB-999_sample.beh"}
     })
     ds.record_row("a",[["x", 1, 0]])
     ds.commit()

     # try something that should work
     # mldb.get asserts the result status_code is >= 200 and < 400
     result = mldb.get("/v1/query", q="select x")

     # assert the result, all unittest asserts are available and
     # assertQueryResult was added to facilitate validating query results
     self.assertQueryResult(result.json(), [{
        'rowName' : 'row1',
        'columns' : [["colA", 1, "1970-01-01T00:00:00Z"]]
     }])

     # test a bad query
     with self.assertRaises(mldb_wrapper.ResponseException) as re:
        mldb.query("SELECT this will not work")

     # the original response is available via re.exception.response
     self.assertEqual(re.exception.response.status_code, 400)

     # directly test the error message
     with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                  'must contain a FROM clause'):
        mldb.query("SELECT *")


mldb.run_tests()