#
# MLDB-1570-procedure-progress.py
# 2016-03-30
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest
import datetime
import time
import tempfile

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
                running = False
            elif resp['state'] == 'executing': # still executing
                self.assertTrue(
                    'progress' in resp,
                    "status is expected to return the progress of the run ")
                self.assertEqual(resp['progress']['steps'][0]['type'],
                                 'percentile')
                self.assertEqual(resp['progress']['steps'][1]['type'],
                                 'percentile')
                if resp['progress']['steps'][0]['name'] == 'iterating':
                    iterating_current_pct = \
                        resp['progress']['steps'][0]['value']
                    bucketizing_current_pct = \
                        resp['progress']['steps'][1]['value']
                else:
                    iterating_current_pct = \
                        resp['progress']['steps'][1]['value']
                    bucketizing_current_pct = \
                        resp['progress']['steps'][0]['value']
                self.assertGreaterEqual(
                    iterating_current_pct, iterating_last_pct,
                    'percent must be increasing')
                iterating_last_pct = iterating_current_pct
                self.assertGreaterEqual(
                    bucketizing_current_pct, bucketizing_last_pct,
                    'percent must be increasing')
                bucketizing_last_pct = bucketizing_current_pct
            time.sleep(0.001)

        self.assertGreater(iterating_last_pct, 0)
        self.assertGreater(bucketizing_last_pct, 0)

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

    def test_import_text_progress(self):
        tmp_file = tempfile.NamedTemporaryFile(dir='build/x86_64/tmp')
        tmp_file.write('a,b,c\n')
        for i in xrange(100000):
            tmp_file.write('{},{},{}\n'.format(i,i * 10, i / 2))
        tmp_file.flush()
        mldb.put('/v1/procedures/import_da_file', {
            'type' : 'import.text',
            'params' : {
                'dataFileUrl' : 'file://' + tmp_file.name,
                'outputDataset' : {
                    'id' : 'ds',
                    'type' : 'tabular'
                }
            }
        })
        location = mldb.post_async("/v1/procedures/import_da_file/runs") \
            .headers['Location']
        res = mldb.get(location).json()
        prev_count = 0
        while res['state'] != 'finished':
            if res['state'] == 'executing':
                self.assertGreaterEqual(res['progress']['value'], prev_count)
                prev_count = res['progress']['value']
            time.sleep(0.001)
            res = mldb.get(location).json()

        self.assertGreater(prev_count, 0)


if __name__ == '__main__':
    mldb.run_tests()
