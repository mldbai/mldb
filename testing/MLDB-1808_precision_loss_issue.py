#
# MLDB-1808_precision_loss_issue.py
# Francois-Michel L Heureux, 2016-07-14
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1808PrecisionLossIssue(MldbUnitTest):  # noqa

    @unittest.expectedFailure
    def test_precision_through_dataset(self):
        number = 71218.50311678024
        data = ['a', number, 0]

        mldb.log(data) # visual inspections shows that data is logged properly

        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('1', [data])
        ds.commit()

        res = mldb.query("SELECT a FROM ds")
        self.assertEqual(res[1][1], number) # mldb yields 71218.5

    def test_precision_straight_in_select(self):
        number = 255650.6226198759
        res = mldb.query("SELECT {:.10f}".format(number))
        self.assertEqual(res[1][1], number)

if __name__ == '__main__':
    mldb.run_tests()
