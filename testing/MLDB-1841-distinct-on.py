# MLDB-1841-distinct-on.py
# Mathieu Marquis Bolduc, 2016-07-21
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class DistinctOnTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "dataset1", "type": "sparse.mutable" })
        ds.record_row("row1",[["x", 1, 0], ["y", 1, 0], ["z", 1, 0]])
        ds.record_row("row2",[["x", 2, 0], ["y", 2, 0], ["z", 1, 0]])
        ds.record_row("row3",[["x", 1, 0], ["y", 3, 0], ["z", 2, 0]])
        ds.record_row("row4",[["x", 1, 0], ["y", 4, 0], ["z", 2, 0]])
        ds.record_row("row5",[["x", 2, 0], ["y", 5, 0], ["z", 3, 0]])
        ds.commit()
   
    def test_distincton_transform(self):

        mldb.post('/v1/procedures', {
            'type': 'transform',
            'params': {
                'inputData': "SELECT DISTINCT ON (x,z) x, z FROM dataset1 ORDER BY x,z",
                'outputDataset': 'transformed',
                'runOnCreation': True
            }
        })

        res = mldb.query("SELECT * from transformed")

        expected = [["_rowName", "x", "z"],
                    ["row1", 1, 1],
                    ["row3", 1, 2],
                    ["row2", 2, 1],
                    ["row5", 2, 3]]

        self.assertEqual(res, expected)


mldb.run_tests()
