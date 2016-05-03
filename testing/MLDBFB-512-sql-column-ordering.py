#
# MLDBFB-512-sql-column-ordering.py
# Mich, 2016-05-02
# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class CountYieldsNullTest(MldbUnitTest):  # noqa

    @unittest.expectedFailure
    def test_it(self):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('user2', [['bucket', '0-100', 0],
                                ['trainConv', 0, 0]])
        ds.record_row('user1', [['bucket', '0-100', 0],
                                ['trainConv', 0, 0],
                                ['testConv', 0, 0]])
        ds.commit()

        query = """SELECT bucket, testConv, count(*), trainConv
                    FROM ds GROUP BY bucket, testConv, trainConv
                    ORDER BY bucket"""

        res = mldb.query(query)
        self.assertEqual(res[0], [
            "_rowName",
            "bucket",
            "testConv",
            "count(*)",
            "trainConv"
        ])

if __name__ == '__main__':
    mldb.run_tests()
