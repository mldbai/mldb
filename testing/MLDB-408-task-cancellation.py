#
# MLDB-408-task-cancellation.py
# Guy Dumais, 2016-09-28
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import time

wrapper = mldb_wrapper.wrap(mldb)  # noqa

class MLDB408TaskCancellation(MldbUnitTest):  # noqa

    def run_procedure_async(self, name, config):
        wrapper.put("/v1/procedures/" + name, config)
        response = wrapper.post_async("/v1/procedures/" + name + "/runs")
        return response.headers['Location']
        
    @classmethod
    def setUpClass(cls):
        # create a dummy dataset
        ds = wrapper.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        row_count = 100000
        for i in xrange(row_count):
            # row name is x's value
            ds.record_row(str(i), [['x', i, 0]])
        ds.commit()

    def test_bucketize_cancellation(self):
        location = self.run_procedure_async('bucketize',  {
            'type' : 'bucketize',
            'params' : {
                'inputData' : 'SELECT * FROM sample ORDER BY x',
                'outputDataset' : {
                    'id' : 'output',
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

        resp = mldb.perform('PUT', location, [], {'state': 'cancelled'});
        mldb.log(resp)

        #self.assertEquals(resp['statusCode'], 200)

        resp = wrapper.get(location).json()
        mldb.log(resp)
        running = True
        while(running):
            resp = wrapper.get(location).json()
            #mldb.log(resp)
            if resp['state'] == 'cancelled' or 'finished':
                running = False
            time.sleep(0.1)

        
if __name__ == '__main__':
    wrapper.run_tests()
