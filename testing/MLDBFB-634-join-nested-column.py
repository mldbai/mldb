# MLDBFB-636-join-rowhash.py
# Mathieu Marquis Bolduc, 2016-07-15
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class DatasetFunctionTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "x", "type": "sparse.mutable" })
        ds.record_row("row1",[[["a", "b"], 1, 0], [["a", "b"], 2, 0]])
        ds.record_row("row2",[[["a", "bééé"], 3, 0], [["c"], 4, 0]])
        ds.record_row("row2",[["a", 3, 0], [[4, 28, "ÉÉÉ"], 4, 0]])
        ds.commit()

    def test_nested(self):        

        mldb.log(mldb.query("""
                             SELECT *
                             FROM x
                             JOIN x AS y
                             ON x.rowHash() = y.rowHash()
                             """))

        mldb.log(mldb.query("""
                            SELECT x.a.b.c
                            FROM x
                            JOIN x AS y
                            ON x.rowHash() = y.rowHash()
                            """))

        mldb.log(mldb.query("""
                            SELECT x.a.b.*
                            FROM x
                            JOIN x AS y
                            ON x.rowHash() = y.rowHash()
                            """))

mldb.run_tests()
