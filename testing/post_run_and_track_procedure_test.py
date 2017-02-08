#
# post_run_and_track_procedure_test.py
# Francois-Michel L'Heureux, 2016-10-18
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class FakeLogger(object):

    def __init__(self, mldb):
        self.records = []
        self.mldb = mldb

    def __enter__(self):
        self.mldb_log = self.mldb.log
        self.mldb.log = self.log
        return self

    def __exit__(self, ex_type, ex_val, tb):
        self.mldb.log = self.mldb_log

    def log(self, msg):
        self.mldb_log(msg)
        self.records.append(str(msg))

class PostRunAndTrackProcedureTest(MldbUnitTest):  # noqa

    def test_it(self):
        with FakeLogger(mldb) as fl:
            mldb.post_run_and_track_procedure({
                'type' : 'mock',
                'params' : {
                    'durationMs' : 4000,
                    'refreshRateMs' : 500
                }
            }, 1)
            self.assertGreater(len(fl.records), 0)

if __name__ == '__main__':
    mldb.run_tests()
