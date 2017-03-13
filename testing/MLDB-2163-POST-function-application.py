#
# MLDB-2163-POST-function-application.py
# Guy Dumais, 2017-03-08
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

expected_output = [
    [ "x", [ 1, "NaD" ] ],
    [ "y", [ 2, "NaD" ] ],
    [ "z", [ "three", "NaD" ] ]
]

class MLDB2163POSTFunctionApplication(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        mldb.put('/v1/functions/query', {
            "type": "sql.query",
            "params": {
                "query": 'select * from row_dataset($row)',
                "output" : 'NAMED_COLUMNS'
            }
        })

    def test_with_get(self):
        res = mldb.get('/v1/functions/query/application', input={'row' : {"x": 1, "y": 2, "z": "three"}})
        mldb.log(res)
        self.assertEquals(res.json()['output']['output'],  expected_output)

    def test_as_POST_body(self):
        res = mldb.post('/v1/redirect/get', {
            'target' : '/v1/functions/query/application',
            'body' : { 
                'input' : {
                    'row' : {"x": 1, "y": 2, "z": "three"}
                }
            }
        })

        self.assertEquals(res.json()['output']['output'], expected_output)

    def test_as_POST_body_async(self):
        """
        The async flag is actually ignore as it does not make 
        sense to call query or function and don't wait for the
        response.
        """
        res = mldb.post_async('/v1/redirect/get', {
            'target' : '/v1/functions/query/application',
            'body' : { 
                'input' : {
                    'row' : {"x": 1, "y": 2, "z": "three"}
                }
            }
        })

        self.assertEquals(res.json()['output']['output'], expected_output)

    def test_as_POST_to_invalid_target(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 
                                     "failed to redirect call"):
            res = mldb.post('/v1/redirect/get', {
                'target' : '/v1/functions/query/application/bla',
                'body' : { 
                    'input' : {
                        'row' : {"x": 1, "y": 2, "z": "three"}
                    }
                }
            })


    def test_as_POST_to_invalid_body(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 
                                     "failed to redirect call"):
            res = mldb.post('/v1/redirect/get', {
                'target' : '/v1/functions/query/application/',
                'body' : { 
                    'bla' : {
                        'row' : {"x": 1, "y": 2, "z": "three"}
                    }
                }
            })


if __name__ == '__main__':
    mldb.run_tests()
