#
# post_run_and_track_procedure_test.py
# Francois-Michel L'Heureux, 2016-10-18
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class FakeLogger(object):

    def __init__(self):
        self.records = []

    def log(self, msg):
        self.records.append(str(msg))

class PostRunAndTrackProcedureTest(MldbUnitTest):  # noqa

    def test_it(self):
        fl = FakeLogger()

        # This test works well alone. mldb.log still points to fl.log after it
        # so if tests need to be added an RAII structure to restore mldb.log
        # would be needed.
        mldb.log = fl.log

        mldb.post_run_and_track_procedure({
            'type' : 'mock',
            'params' : {
                'durationMs' : 4000,
                'refreshRateMs' : 500
            }
        }, 1)
        self.assertGreaterThan(len(fl.records), 0)

if __name__ == '__main__':
    mldb.run_tests()
