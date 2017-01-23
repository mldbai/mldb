#
# serial_procedure_test.py
# mldb.ai inc, 2015
# this file is part of mldb. copyright 2015 mldb.ai inc. all rights reserved.
#
import time

mldb = mldb_wrapper.wrap(mldb) # noqa

class SerialProcedureTest(MldbUnitTest):  # noqa

    def test_deadloack(self):
        # MLDB-621
        try:
            mldb.perform("PUT", "/v1/procedures/q", [], {
                "type": "serial",
                "params": {"steps": [{"id": "q", "type": "null"}]}
            })
        except Exception:
            pass

    def test_progress(self):
        proc = {
            'type' : 'mock',
            'params' : {
                'durationMs' : 900,
                'refreshRateMs' : 100
            }
        }

        res = mldb.post_async('/v1/procedures', {
            'type' : "serial",
            'params' : {
                'steps' : [proc, proc, proc, proc, proc]
            }
        }).json()
        proc_id = res['id']
        run_id = res['status']['firstRun']['id']
        time.sleep(0.5)
        url = '/v1/procedures/{}/runs/{}'.format(proc_id, run_id)
        res = mldb.get(url).json()
        self.assertEqual(res['state'], 'executing')
        self.assertTrue('subProgress' in res['progress'])
        self.assertEqual(len(res['progress']['steps']), 5)

        def reducer(x, y):
            return x + y['value']

        total1 = reduce(reducer, res['progress']['steps'], 0)

        time.sleep(1)
        res = mldb.get(url).json()
        total2 = reduce(reducer, res['progress']['steps'], 0)
        self.assertGreater(total2, total1)

if __name__ == '__main__':
    mldb.run_tests()
