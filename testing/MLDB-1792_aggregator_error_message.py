#
# MLDB-1792_aggregator_error_message.py
# Francois-Michel L Heureux, 2016-07-11
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1792AggregatorErrorMessage(MldbUnitTest):  # noqa

    def test_it(self):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['colA', 1, 0]])
        ds.commit()

        msg = "function avg expected 1 argument, got 2"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT avg(colA, 2) FROM ds")

if __name__ == '__main__':
    mldb.run_tests()
