#
# alias_resolving_test.py
# Francois-Michel L Heureux, 2016-06-21
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb)  # noqa

class AliasResolvingTest(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({'id' : 'a', 'type' : 'sparse.mutable'})
        ds.commit()

        ds = mldb.create_dataset({'id' : 'b', 'type' : 'sparse.mutable'})
        ds.commit()

    @unittest.expectedFailure
    def test_bad_alias_lhs_inner_join(self):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.query("""
                SELECT *
                FROM a
                INNER JOIN b ON undefined.rowName() = b.rowName()
            """)

    @unittest.expectedFailure
    def test_bad_alias_rhs_inner_join(self):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.query("""
                SELECT *
                FROM a
                INNER JOIN b ON a.rowName() = undefined.rowName()
            """)

    @unittest.expectedFailure
    def test_bad_alias_lhs_where(self):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.query("""
                SELECT *
                FROM a
                WHERE undefined.rowName()=123
            """)

    @unittest.expectedFailure
    def test_bad_alias_rhs_where(self):
        with self.assertRaises(mldb_wrapper.ResponseException):
            mldb.query("""
                SELECT *
                FROM a
                WHERE a.rowName()=undefined.column
            """)


if __name__ == '__main__':
    mldb.run_tests()
