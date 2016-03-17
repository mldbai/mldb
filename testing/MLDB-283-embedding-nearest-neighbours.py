#
# MLDB-283-embedding-nearest-neighbours.py
# 2016-03-14
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#


import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb283Test(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "test", "type": "embedding" })
        
        ts = 0
        ds.record_row("ex1", [ [ "x", 0, ts ], ["y", 0, ts]])
        ds.record_row("ex2", [ [ "x", 0, ts ], ["y", 1, ts]])
        ds.record_row("ex3", [ [ "x", 1, ts ], ["y", 0, ts]])
        ds.record_row("ex4", [ [ "x", 1, ts ], ["y", 1, ts]])

        ds.commit()

        # create nn function
        mldb.put("/v1/functions/nn", {
            "type": 'nearest.neighbors',
            "params": {
                'dataset': {"id": 'test', "type": "embedding"}
            }
        })

    def test_select(self):
        self.assertTableResultEquals(
            mldb.query("select nn({coords: [0.5, 0.5]})[neighbors] as *"),
            [
                ["_rowName", "ex1", "ex2", "ex3", "ex4"],
                ["",  0.7071067690849304,
                      0.7071067690849304,
                      0.7071067690849304,
                      0.7071067690849304 ]
            ]
        )
        
        self.assertTableResultEquals(
            mldb.query("select nn({coords: [0.1, 0.2]})[neighbors] as *"),
            [
                ["_rowName", "ex1", "ex2", "ex3", "ex4"],
                ["",  0.22360680997371674,
                        0.8062257766723633,
                        0.9219543933868408,
                        1.2041594982147217]
            ]
        )

    def test_select_row(self):
        # MLDB-509
        self.assertTableResultEquals(
            mldb.query("select nn({coords: 'ex1'})[neighbors] as *"),
            [
                ["_rowName", "ex1", "ex2", "ex3", "ex4"],
                ["",  0, 1, 1, 1.4142135381698608]
            ]
        )

    def test_select_params(self):
        self.assertTableResultEquals(
            mldb.query("select nn({coords: 'ex1', num_neighbours:2})[neighbors] as *"),
            [
                ["_rowName", "ex1", "ex2"],
                ["",  0, 1]
            ]
        )
        
        self.assertTableResultEquals(
            mldb.query("select nn({coords: 'ex1', num_neighbours:2, max_distance:0.5})[neighbors] as *"),
            [
                ["_rowName", "ex1"],
                ["",  0]
            ]
        )

mldb.run_tests()

