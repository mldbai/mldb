#
# MLDB-1490-grouped-validation.py
# 2016-03-25
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class InvalidGroupByTest(MldbUnitTest):  

    @classmethod
    def setUpClass(self):
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        ds.record_row("a",[["x", 1, 0]])
        ds.commit()

    def test_valid_group_by(self):
        mldb.query("select count(*), sum(deletions) from sample group by x")

    def test_valid_builtin_on_aggregator(self):
        mldb.query("select count(*), ln(sum(deletions)+1) from sample group by x")

    def test_valid_aggregator_on_builtin(self):
        mldb.query("select count(*), sum(ln(deletions+1)) from sample group by x")

    def test_valid_builtin_on_aggregator_on_builtin(self):
        mldb.query("select count(*), ln(sum(deletions)+1) from sample")

    def test_valid_aggregator_on_builtin_on_op(self):
        mldb.query("select count(*), sum(ln(deletions+1)) from sample")

    def test_valid_aggregator_on_wildcard_builtin(self):
        mldb.query("select count(*), earliest(temporal_earliest({*})) from sample group by x")
        mldb.query("select count(*), earliest({horizontal_earliest({*})}) from sample group by x")

    def test_invalid_aggregator_and_builtin(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, 
                                     "variable 'deletions' must appear in the GROUP BY clause .*"):
            mldb.query("select count(*), ln(deletions+1) from sample group by x")

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     "variable 'deletions' must appear in the GROUP BY clause .*"):
            mldb.query("select count(*), ln(deletions+1) from sample")

    def test_invalid_aggregator_and_wildcard_builtin(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     "Non-aggregator 'temporal_earliest\(\{\*\}\)' with GROUP BY clause is not allowed"):
            mldb.query("select count(*), temporal_earliest({*}) from sample group by x")

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     "Mixing non-aggregator 'horizontal_earliest\(\{\*\}\)' with aggregators is not allowed"):
            mldb.query("select count(*), horizontal_earliest({*}) from sample")

    def test_invalid_aggregator_and_many_wildcard_builtin(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     "Non-aggregator 'temporal_earliest\(\{\*\}\)' with GROUP BY clause is not allowed"):
            mldb.query("""select count(*) as cnt, 
            x, 
            temporal_earliest({*}) as earliest, 
            temporal_latest({*}) as latest, 
            sum(filesChanged) as changes, 
            sum(insertions) as insertions, 
            sum(deletions) as deletions from sample group by x""")

    def test_invalid_group_by_and_wildcard_builtin(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     "Non-aggregator 'temporal_earliest\(\{\*\}\)' with GROUP BY clause is not allowed"):
            mldb.query("select temporal_earliest({*}) as earliest from sample group by x")

    @unittest.skip('MLDBFB-435')
    def test_invalid_nested_aggregators(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                     "Nested aggregators 'earliest(earliest({*}))' are not allowed"):
            mldb.query("select count(*), earliest(earliest({*})) from sample")

mldb.run_tests()
