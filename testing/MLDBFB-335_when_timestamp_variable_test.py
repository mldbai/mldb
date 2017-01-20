#
# MLDBFB-335_when_timestamp_variable_test.py
# Mich, 2016-02-02
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class WhenValueTest(MldbUnitTest):

    t = '1970-01-01T00:00:01Z'

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({
            'id' : 'ds',
            'type' : 'sparse.mutable'
        })
        ds.record_row('row1', [['colA', 1, 0], ['colB', 1, 1], ['colC', 1, 2]])
        ds.commit()

        ds = mldb.create_dataset({
            'id' : 'timeDs',
            'type' : 'sparse.mutable'
        })
        ds.record_row('row1', [['time', cls.t, 0]])
        ds.commit()

    def test_it(self):
        # t is indeed equal to the stored timestamp
        t = self.__class__.t
        res = mldb.query("SELECT time FROM timeDs")
        self.assertEqual(res[1][1], t)

        # but fetching via a value vs a variable containing the same value
        # doesn't work
        res_value = mldb.query(
            "SELECT * FROM merge(ds, timeDs) WHEN value_timestamp() <= TIMESTAMP '{}'"
            .format(t))
        res_variable = mldb.query(
            "SELECT * FROM merge(ds, timeDs) WHEN value_timestamp() <= TIMESTAMP time")
        self.assertTableResultEquals(res_value, res_variable)

if __name__ == '__main__':
    mldb.run_tests()
