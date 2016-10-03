#
# MLDB-408-task-cancellation.py
# Guy Dumais, 2016-09-28
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
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
        
    @classmethod
    def setUpClass(cls):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        row_count = 1000000
        for i in xrange(row_count):
            # row name is x's value
            ds.record_row(str(i), [['x', i, 0]])
        ds.commit()

    @timed
    def test_bucketize_cancellation(self):
        location = self.run_procedure_async('bucketize',  {
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

        resp = mldb.get(location)
        mldb.log(resp)

        resp = mldb.put(location, {'state': 'cancelled'})

        self.assertEquals(resp.status_code, 200)

        running = True
        while(running):
            resp = mldb.get(location).json()
            mldb.log(resp)
            if resp['state'] == 'cancelled':
                running = False
            sleep(0.1)

        
    @timed
    def test_transform_cancellation(self):
        location = self.run_procedure_async('transform',  {
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

        resp = mldb.get(location)
        mldb.log(resp)

        resp = mldb.put(location, {'state': 'cancelled'})

        self.assertEquals(resp.status_code, 200)

        running = True
        while(running):
            resp = mldb.get(location).json()
            mldb.log(resp)
            if resp['state'] == 'cancelled':
                running = False
            sleep(0.1)


if __name__ == '__main__':
    mldb.run_tests()
