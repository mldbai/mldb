#
# MLDBFB-401_where_on_unexisting_col_test.py
# Mich, 2016-03-14
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class QueryWhereTest(MldbUnitTest):  # noqa

    def test_where_on_unexisting_col_in_sparse(self):
        mldb.create_dataset({'id' : 'sparse', 'type' : 'sparse.mutable'}).commit()
        mldb.query("SELECT * FROM sparse WHERE someCol IS NOT NULL")
        mldb.query("SELECT * FROM sparse WHERE \"someCol\" IS NOT NULL")

    def test_where_on_unexisting_col_in_beh(self):
        mldb.create_dataset({'id' : 'beh', 'type' : 'beh.mutable'}).commit()
        mldb.query("SELECT * FROM beh WHERE someCol IS NOT NULL")
        mldb.query("SELECT * FROM beh WHERE \"someCol\" IS NOT NULL")

    def test_where_on_unexisting_col_in_beh_binary(self):
        mldb.create_dataset({'id' : 'binary', 'type' : 'beh.binary.mutable'}).commit()
        mldb.query("SELECT * FROM binary WHERE someCol IS NOT NULL")
        mldb.query("SELECT * FROM binary WHERE \"someCol\" IS NOT NULL")


if __name__ == '__main__':
    mldb.run_tests()
