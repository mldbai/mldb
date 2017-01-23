#
# MLDB-1713-wildcard-groupby.py
# Mathieu Bolduc, 2016-08-15
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1713WildcardGroupby(MldbUnitTest):  # noqa

    def test_wildcard_groupby(self):
        msg = "Wildcard cannot be used with GROUP BY"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query('select * from (select 1 as a) group by a')

if __name__ == '__main__':
    mldb.run_tests()
