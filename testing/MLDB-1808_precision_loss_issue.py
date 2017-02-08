#
# MLDB-1808_precision_loss_issue.py
# Francois-Michel L Heureux, 2016-07-14
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1808PrecisionLossIssue(MldbUnitTest):  # noqa

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

    def test_precision_in_agg(self):
        ds = mldb.create_dataset({'id' : 'ds_agg', 'type' : 'sparse.mutable'})
        number_1 =  71218.50311678024
        number_2 = 255650.6226198759
        ds.record_row('1', [['a', number_1, 0]])
        ds.record_row('2', [['a', number_2, 0]])
        ds.commit()

        res = mldb.query("SELECT sum(a) FROM ds_agg")
        self.assertEqual(res[1][1], number_1 + number_2)

        res = mldb.query("SELECT avg(a) FROM ds_agg")
        self.assertEqual(res[1][1], (number_1 + number_2) / 2)


if __name__ == '__main__':
    mldb.run_tests()
