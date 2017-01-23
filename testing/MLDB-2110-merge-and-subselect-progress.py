# MLDB-2110-merge-and-subselect-progress.py
# mldb.ai inc, 2017
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

import time

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb2110MergeProgressTest(MldbUnitTest):

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        for i in range(0,500000):
            ds.record_row('row'+str(i), [['a', i, 4], ['b', -i, 4]]) 

        ds.commit()

    def test_progress(self):
        res = mldb.post_async('/v1/procedures', {
            'type' : 'transform',
            'params' : {
                'inputData' : """SELECT * FROM merge( 
                    (SELECT l FROM ( 
                        SELECT sum - diff AS l FROM ( 
                            SELECT b + a AS sum, b -a AS diff FROM ds
                           )
                        ) 
                     ),

                     (SELECT r FROM ( 
                        SELECT sum + diff AS r FROM ( 
                            SELECT a + b AS sum, a - b AS diff FROM ds)
                        )
                     )
                )""",
                'outputDataset' : 'ds_op',
                'runOnCreation' : True
            }
        }).json()

        url = '/v1/procedures/{}/runs/{}'.format(
            res['id'], res['status']['firstRun']['id'])
        last_percent = 0
        while True:
            res = mldb.get(url).json()
           
            mldb.log(res)
            if res['state'] == 'finished':
                break
            if res['state'] == 'executing':
                percent = res['progress']['steps'][0]['value']
                self.assertGreaterEqual(percent, last_percent)
                last_percent = percent

            time.sleep(0.5)

if __name__ == '__main__':
    mldb.run_tests()

