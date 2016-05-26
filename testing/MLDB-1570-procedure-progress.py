#
# MLDB-1570-procedure-progress.py
# 2016-03-30
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest
import datetime
import time

mldb = mldb_wrapper.wrap(mldb)

class ProcedureProgressTest(MldbUnitTest):

    def run_procedure_async(self, name, config):
        mldb.put("/v1/procedures/" + name, config)
        response = mldb.post_async("/v1/procedures/" + name + "/runs")
        return response.headers['Location']

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        row_count = 10000
        for i in xrange(row_count):
            # row name is x's value
            ds.record_row(str(i), [['x', i, datetime.datetime.now()]])
        ds.commit()

    def test_bucketize_progress(self):
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
                }
            }
        })

        running = True
        iterating_last_pct = 0.0
        bucketizing_last_pct = 0.0
        while(running):
            resp = mldb.get(location).json()
            self.assertTrue('id' in resp,
                            "status is expected to return the id of the run")
            self.assertTrue(
                'state' in resp,
                "status is expected to return the state of the run")
            if resp['state'] == 'finished':
                mldb.log(resp)
                running = False
            elif resp['state'] == 'executing': # still executing
                self.assertTrue(
                    'progress' in resp,
                    "status is expected to return the progress of the run "
                    + str(resp))
                if resp['progress']['steps'][0]['name'] == 'iterating':
                    iterating_current_pct = \
                        resp['progress']['steps'][0]['percent']
                    bucketizing_current_pct = \
                        resp['progress']['steps'][1]['percent']
                else:
                    iterating_current_pct = \
                        resp['progress']['steps'][1]['percent']
                    bucketizing_current_pct = \
                        resp['progress']['steps'][0]['percent']
                self.assertGreaterEqual(
                    iterating_current_pct, iterating_last_pct,
                    'percent must be increasing')
                iterating_last_pct = iterating_current_pct
                self.assertGreaterEqual(
                    bucketizing_current_pct, bucketizing_last_pct,
                    'percent must be increasing')
                bucketizing_last_pct = bucketizing_current_pct
                mldb.log(resp)
            time.sleep(0.001)

        resp = mldb.put("/v1/procedures/test", {
            'type' : 'bucketize',
            'params' : {
                'inputData' : 'SELECT * FROM sample ORDER BY x',
                'outputDataset' : {
                    'id' : 'output',
                    'type' : 'sparse.mutable'
                },
                'percentileBuckets': {'b1': [0, 50], 'b2': [50, 100]}
            }
        })

        resp = mldb.post("/v1/procedures/test/runs")
        mldb.log(resp)

if __name__ == '__main__':
    mldb.run_tests()
