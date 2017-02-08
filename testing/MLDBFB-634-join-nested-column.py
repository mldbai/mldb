# MLDBFB-636-join-rowhash.py
# Mathieu Marquis Bolduc, 2016-07-15
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class DatasetFunctionTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "x", "type": "sparse.mutable" })
        ds.record_row("row1",[[["a", "b"], 1, 0], [["a", "c"], 2, 0]])
        ds.record_row("row2",[[["a", "b"], 3, 0], [["a", "c"], 4, 0]])
        ds.commit()

    def test_nested(self):

        res = mldb.query("""
                            SELECT x.a.*
                            FROM x
                            JOIN x AS y
                            ON x.rowHash() = y.rowHash()
                            """
                        )

        expected = [["_rowName", "x.a.b", "x.a.c"],
                    ["[row1]-[row1]", 1, 2],
                    ["[row2]-[row2]", 3, 4]]

        self.assertTableResultEquals(res, expected)

    def test_nested_pipeline(self):

        res = mldb.put('/v1/functions/nested', {
                    'type': 'sql.query',
                    'params': {
                    'query': 'SELECT x.a.* FROM x JOIN x AS y ON x.rowHash() = y.rowHash()'
                    }})

        res = mldb.query('SELECT nested()')

        mldb.log(res)

        expected = [["_rowName", "nested().x.a.b", "nested().x.a.c"],
                    ["result", 1, 2 ]]

        self.assertTableResultEquals(res, expected)

mldb.run_tests()
