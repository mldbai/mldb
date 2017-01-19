#
# MLDB-2064_transform_proc_row_expr.py
# Francois Maillet, 2016-11-29
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2064TransformProcRowExpr(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        pass

    def test_it(self):
        query =  """
                    SELECT * FROM 
                        row_dataset({
                            "My Value": 1
                        })
                """

        good = mldb.query(query)

        print mldb.post('/v1/procedures', {
            'type': 'transform',
            'params': {
                'inputData': query,
                'outputDataset': {"id": 'keywords', "type": "tabular"}
            }
        })


        self.assertTableResultEquals(
             mldb.query("select * from keywords"),
             good)
  

if __name__ == '__main__':
    mldb.run_tests()
