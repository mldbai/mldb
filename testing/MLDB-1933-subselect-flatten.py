#
# MLDB-1933-subselect-flatten.py
# Mathieu Marquis Bolduc, 2016-09-08
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class ColumnExprTest(MldbUnitTest):  # noqa

    def test_SubDataset(self):        

        # should see two columns if unflattened, 4 if flattened
        mldb.log(
        mldb.query("SELECT COLUMN EXPR (SELECT 1) FROM (SELECT [[2,3],[4,5]] as myembedding)")
        )

if __name__ == '__main__':
    mldb.run_tests()