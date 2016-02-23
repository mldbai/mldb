# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

#
# MLDBFB-336-sample_test.py
# 2016-01-01
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

# add this line to testing.mk:
# $(eval $(call mldb_unit_test,MLDBFB-336-sample_test.py,,manual))


import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class RowAggregatorTest(MldbUnitTest):  

    ts = "2015-01-01T00:00:00Z";

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "test", "type": "sparse.mutable" })

        def recordExample(row, x, y, label):
            ds.record_row(row, [ [ "x", x, self.ts ], ["y", y, self.ts], ["label", label, self.ts] ]);

        recordExample("ex1", 0, 0, "cat");
        recordExample("ex2", 1, 1, "dog");
        recordExample("ex3", 1, 2, "cat");

        ds.commit()

        
    def test_min_max(self):
        resp = mldb.get("/v1/query", q = "SELECT min({*}) AS min, max({*}) AS max FROM test GROUP BY label");
        self.assertFullResultEquals(resp.json(),
                                    [
                                        {
                                            "columns" : [
                                            [ "min.label", "cat", self.ts ],
                                                [ "min.x", 0, self.ts ],
                                                [ "min.y", 0, self.ts ],
                                                [ "max.label", "cat", self.ts ],
                                                [ "max.x", 1, self.ts ],
                                                [ "max.y", 2, self.ts ]
                                            ],
                                            "rowHash" : "554f96c80ea05ddb",
                                            "rowName" : "[\"cat\"]"
                                        },
                                        {
                                            "columns" : [
                                                [ "min.label", "dog", self.ts ],
                                                [ "min.x", 1, self.ts ],
                                                [ "min.y", 1, self.ts ],
                                                [ "max.label", "dog", self.ts ],
                                                [ "max.x", 1, self.ts ],
                                                [ "max.y", 1, self.ts ]
                                            ],
                                            "rowHash" : "d55e0e284796f79e",
                                            "rowName" : "[\"dog\"]"
                                    }
                                    ]);


    def test_mldb_988(self): #MLDB-988
        resp = mldb.get("/v1/query", q = "SELECT sum(x) AS sum FROM test GROUP BY x");
        self.assertFullResultEquals(resp.json(),
                                    [
                                        {
                                            "rowName": "[0]",
                                            "rowHash": "1d9a5ddf40663f6b",
                                            "columns": [ [ "sum", 0, "2015-01-01T00:00:00Z" ] ]
                                        },
                                        {
                                            "rowName": "[1]",
                                            "rowHash": "2d7ea86e36813b82",
                                            "columns": [ [ "sum", 2, "2015-01-01T00:00:00Z" ] ]
                                        }
                                    ]);

    def test_vertical_sum_is_sum(self):
        resp = mldb.get("/v1/query", q = "SELECT sum(x) AS sum FROM test GROUP BY x");
        resp2 = mldb.get("/v1/query", q = "SELECT vertical_sum(x) AS sum FROM test GROUP BY x");
        self.assertFullResultEquals(resp.json(), resp2.json())

    def test_vertical_count_is_count(self):
        resp = mldb.get("/v1/query", q = "SELECT count(x) AS count FROM test GROUP BY x");
        resp2 = mldb.get("/v1/query", q = "SELECT vertical_count(x) AS count FROM test GROUP BY x");
        self.assertFullResultEquals(resp.json(), resp2.json())

    def test_vertical_count_is_count_star(self):
        resp = mldb.get("/v1/query", q = "SELECT count(*) AS count FROM test GROUP BY x");
        resp2 = mldb.get("/v1/query", q = "SELECT vertical_count(*) AS count FROM test GROUP BY x");
        self.assertFullResultEquals(resp.json(), resp2.json())

    def test_vertical_avg_is_avg(self):
        resp = mldb.get("/v1/query", q = "SELECT avg(x) AS avg FROM test GROUP BY x");
        resp2 = mldb.get("/v1/query", q = "SELECT vertical_avg(x) AS avg FROM test GROUP BY x");
        self.assertFullResultEquals(resp.json(), resp2.json())

mldb.run_tests()
