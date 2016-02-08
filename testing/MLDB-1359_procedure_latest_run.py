#
# MLDB-1359_procedure_latest_run.py
# Mich, 2016-02-05
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
import time
from dateutil import parser as date_parser

mldb = mldb_wrapper.wrap(mldb) # noqa


class ProcedureLatetRunTest(MldbUnitTest): # noqa

    def test_it(self):
        ds = mldb.create_dataset({
            'id' : 'ds',
            'type' : 'sparse.mutable',
        })
        ds.record_row('row1', [['colA', 1, 1]])
        ds.commit()

        url = '/v1/procedures/testProc'
        mldb.put(url, {
            'type' : 'transform',
            'params' : {
                'inputData' : 'SELECT *, coco AS sanchez FROM ds',
                'outputDataset' : {
                    'id' : 'dsOut'
                },
                'runOnCreation' : True
            }
        })

        res = mldb.get(url + '/latestrun').json()
        run_date = date_parser.parse(res['runStarted'])
        mldb.log(run_date)

        time.sleep(0.01)
        mldb.put(url + '/runs/999')
        new_res = mldb.get(url + '/latestrun').json()
        latest_run_date = date_parser.parse(new_res['runStarted'])
        self.assertGreater(latest_run_date, run_date)

        run_date = latest_run_date
        time.sleep(0.01)
        mldb.post(url + '/runs')
        new_res = mldb.get(url + '/latestrun').json()
        latest_run_date = date_parser.parse(new_res['runStarted'])
        self.assertGreater(latest_run_date, run_date)

        run_date = latest_run_date
        time.sleep(0.01)
        mldb.put(url + '/runs/000')
        new_res = mldb.get(url + '/latestrun').json()
        latest_run_date = date_parser.parse(new_res['runStarted'])
        self.assertGreater(latest_run_date, run_date)
        self.assertEqual(new_res['id'], '000')

if __name__ == '__main__':
    mldb.run_tests()
