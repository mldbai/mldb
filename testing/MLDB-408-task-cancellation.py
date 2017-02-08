#
# MLDB-408-task-cancellation.py
# Guy Dumais, 2016-09-28
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

from time import time, sleep
from functools import wraps

mldb = mldb_wrapper.wrap(mldb)  # noqa

def timed(f):
  @wraps(f)
  def wrapper(*args, **kwds):
    start = time()
    result = f(*args, **kwds)
    elapsed = time() - start
    mldb.log("%s took %f seconds to finish" % (f.__name__, elapsed))
    return result
  return wrapper

class MLDB408TaskCancellation(MldbUnitTest):  # noqa

    def run_procedure_async(self, name, config):
        mldb.put("/v1/procedures/" + name, config)
        response = mldb.post_async("/v1/procedures/" + name + "/runs")
        mldb.log(response)
        return response.headers['Location']
        
    def run_and_cancel(self, name, config):
        location = self.run_procedure_async(name, config)

        resp = mldb.put(location + "/state", {'state': 'cancelled'})

        self.assertEquals(resp.status_code, 200)

        running = True
        while running:
            resp = mldb.get(location + "/state")
            mldb.log(resp)

            if resp.json()['state'] == 'cancelled':
                running = False
            if resp.json()['state'] == 'finished':
                self.fail("suspicious - the procedure finished before it was cancelled")
            if resp.json()['state'] == 'error':
                mldb.log(mldb.get(location))
                self.fail("the procedure generated an error before or after cancellation")

            sleep(0.1)


    @classmethod
    def setUpClass(cls):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })

        row_count = 1000000
        for i in xrange(row_count):
            # row name is x's value
            ds.record_rows([ [str(i*10+1), [['x', i*10 + 1, 0], ['y', i*10 + 3, 0]]],
                             [str(i*10+2), [['x', i*10 + 2, 0], ['y', i*10 + 3, 0]]],
                             [str(i*10+3), [['x', i*10 + 3, 0], ['y', i*10 + 3, 0]]],
                             [str(i*10+4), [['x', i*10 + 4, 0], ['y', i*10 + 3, 0]]],
                             [str(i*10+5), [['x', i*10 + 5, 0], ['y', i*10 + 3, 0]]],
                             [str(i*10+6), [['x', i*10 + 6, 0], ['y', i*10 + 3, 0]]],
                             [str(i*10+7), [['x', i*10 + 7, 0], ['y', i*10 + 3, 0]]],
                             [str(i*10+8), [['x', i*10 + 8, 0], ['y', i*10 + 3, 0]]],
                             [str(i*10+9), [['x', i*10 + 9, 0], ['y', i*10 + 3, 0]]],
                             [str(i*10+0), [['x', i*10 + 0, 0], ['y', i*10 + 3, 0]]]
                           ])

        ds.commit()

    @timed
    def test_bucketize_cancellation(self):
        self.run_and_cancel('bucketize',  {
            'type' : 'bucketize',
            'params' : {
                'inputData' : 'SELECT * FROM sample ORDER BY x',
                'outputDataset' : {
                    'id' : 'bucketize_output',
                    'type' : 'sparse.mutable'
                },
                'percentileBuckets': {
                    'b1': [0, 25],
                    'b2': [25, 50],
                    'b3': [50, 75],
                    'b4': [75, 100]
                },
                'runOnCreation' : False
            }
        })
        
    @timed
    def test_transform_cancellation(self):
        self.run_and_cancel('transform',  {
            'type' : 'transform',
            'params' : {
                'inputData' : 'SELECT x + 10 FROM sample',
                'outputDataset' : {
                    'id' : 'transform_output',
                    'type' : 'sparse.mutable'
                },
                'runOnCreation' : False
            }
        })


    @timed
    def test_group_by_transform_cancellation(self):
        self.run_and_cancel('group_by_transform',  {
            'type' : 'transform',
            'params' : {
                'inputData' : 'SELECT x + 10 FROM sample GROUP BY x',
                'outputDataset' : {
                    'id' : 'transform_output',
                    'type' : 'sparse.mutable'
                },
                'runOnCreation' : False
            }
        })


    @timed
    def test_svd_train_cancellation(self):
        self.run_and_cancel('svd.train', {
            'type' : 'svd.train',
            'params' : {
                "trainingData": 'select * from sample',
                "columnOutputDataset": {
                    "type": "sparse.mutable",
                    "id": "column"
                },
                'runOnCreation' : False
            }
        })


if __name__ == '__main__':
    mldb.run_tests()
