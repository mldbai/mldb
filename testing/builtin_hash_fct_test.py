#
# builtin_hash_fct_test.py
# Francois-Michel L Heureux, 2016-07-08
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

from mldb import mldb, MldbUnitTest, ResponseException

class BuiltinHashFctTest(MldbUnitTest):  # noqa

    def test_it(self):
        res = mldb.query("SELECT hash(1)")
        self.assertEqual(res[1][1], 6057722525893115906)

        res = mldb.query("SELECT hash('1')")
        self.assertEqual(res[1][1], 10592438303399217743)

        res = mldb.query("SELECT hash('abc')")
        self.assertEqual(res[1][1], 17130090052465147157)

        res = mldb.query("SELECT hash({a: 12, b: 'coco'})")
        self.assertEqual(res[1][1], 4703301921268040991)

        res = mldb.query("SELECT hash(NULL)")
        self.assertEqual(res[1][1], None)

    def test_with_ts(self):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('row1', [['a', 1, 0], ['b', 1, 1]])
        ds.commit()

        # different timestamps yield same hash
        res = mldb.query("SELECT hash(a) = hash(b) FROM ds")
        self.assertEqual(res[1][1], 1)


if __name__ == '__main__':
    mldb.run_tests()
