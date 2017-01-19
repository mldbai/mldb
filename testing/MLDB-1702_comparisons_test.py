#
# MLDB-1702_comparisons_test.py
# Mich, 2016-06-02
# Copyright (c) 2016 mldb.ai inc. All rights reserved.
#
import unittest
from functools import partial

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldb1702ComparisonTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'ds', 'type' : 'sparse.mutable'})
        ds.record_row('u1', [['a', 1, 4],
                             ['b', 1, 4], ['b', 1, 5],
                             ['c', 0, 4], ['c', 0, 5],
                             ['d', 0, 4], ['d', 1, 5],
                             ['e', 1, 4], ['e', 0, 5]])
        ds.commit()

    def run_query(self, letter, operator, value):
        return mldb.query("""
            SELECT "{letter}" {operator} {value} FROM ds
        """.format(**locals()))[1][1]


    def test_single_val(self):
        rq = partial(self.run_query, 'a')
        self.assertEqual(rq('>', 1), 0)
        self.assertEqual(rq('>', 0), 1)
        self.assertEqual(rq('>=', 1), 1)
        self.assertEqual(rq('>=', 0), 1)
        self.assertEqual(rq('=', 1), 1)
        self.assertEqual(rq('=', 0), 0)
        self.assertEqual(rq('<=', 1), 1)
        self.assertEqual(rq('<=', 0), 0)
        self.assertEqual(rq('<', 1), 0)
        self.assertEqual(rq('<', 0), 0)

        self.assertEqual(rq('!=', 1), 0)
        self.assertEqual(rq('!=', 0), 1)

    def test_two_ones(self):
        rq = partial(self.run_query, 'b')
        self.assertEqual(rq('>', 1), 0)
        self.assertEqual(rq('>', 0), 1)
        self.assertEqual(rq('>=', 1), 1)
        self.assertEqual(rq('>=', 0), 1)
        self.assertEqual(rq('=', 1), 1)
        self.assertEqual(rq('=', 0), 0)
        self.assertEqual(rq('<=', 1), 1)
        self.assertEqual(rq('<=', 0), 0)
        self.assertEqual(rq('<', 1), 0)
        self.assertEqual(rq('<', 0), 0)

        self.assertEqual(rq('!=', 1), 0)
        self.assertEqual(rq('!=', 0), 1)

    def test_two_zeroes(self):
        rq = partial(self.run_query, 'c')
        self.assertEqual(rq('>', 1), 0)
        self.assertEqual(rq('>', 0), 0)
        self.assertEqual(rq('>=', 1), 0)
        self.assertEqual(rq('>=', 0), 1)
        self.assertEqual(rq('=', 1), 0)
        self.assertEqual(rq('=', 0), 1)
        self.assertEqual(rq('<=', 1), 1)
        self.assertEqual(rq('<=', 0), 1)
        self.assertEqual(rq('<', 1), 1)
        self.assertEqual(rq('<', 0), 0)

        self.assertEqual(rq('!=', 1), 1)
        self.assertEqual(rq('!=', 0), 0)

    def test_zero_to_one(self):
        rq = partial(self.run_query, 'd')
        self.assertEqual(rq('>', 1), 0)
        self.assertEqual(rq('>', 0), 1)
        self.assertEqual(rq('>=', 1), 1)
        self.assertEqual(rq('>=', 0), 1)
        self.assertEqual(rq('=', 1), 1)
        self.assertEqual(rq('=', 0), 0)
        self.assertEqual(rq('<=', 1), 1)
        self.assertEqual(rq('<=', 0), 0)
        self.assertEqual(rq('<', 1), 0)
        self.assertEqual(rq('<', 0), 0)

        self.assertEqual(rq('!=', 1), 0)
        self.assertEqual(rq('!=', 0), 1)

    def test_one_to_zero(self):
        rq = partial(self.run_query, 'e')
        self.assertEqual(rq('>', 1), 0)
        self.assertEqual(rq('>', 0), 0)
        self.assertEqual(rq('>=', 1), 0)
        self.assertEqual(rq('>=', 0), 1)
        self.assertEqual(rq('=', 1), 0)
        self.assertEqual(rq('=', 0), 1)
        self.assertEqual(rq('<=', 1), 1)
        self.assertEqual(rq('<=', 0), 1)
        self.assertEqual(rq('<', 1), 1)
        self.assertEqual(rq('<', 0), 0)

        self.assertEqual(rq('!=', 1), 1)
        self.assertEqual(rq('!=', 0), 0)

if __name__ == '__main__':
    mldb.run_tests()
