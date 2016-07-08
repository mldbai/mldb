#
# hash_test.py
# Francois-Michel L Heureux, 2016-07-08
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class HashTest(MldbUnitTest):  # noqa

    def test_it(self):
        res = mldb.query("SELECT hash(1)")
        self.assertEqual(res[1][1], 10234557827792954321)

        res = mldb.query("SELECT hash('1')")
        self.assertEqual(res[1][1], 8353419265319147257)

        res = mldb.query("SELECT hash({a: 12, b: 'coco'})")
        self.assertEqual(res[1][1], 11927858061408965740)

        res = mldb.query("SELECT hash(NULL)")
        self.assertEqual(res[1][1], None)

    def test_with_ts(self):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['a', 1, 0], ['b', 1, 1]])
        ds.commit()

        # different timestamp yields same hash
        res = mldb.query("SELECT hash(a) = hash(b) FROM ds")
        self.assertEqual(res[1][1], 1)


if __name__ == '__main__':
    mldb.run_tests()
