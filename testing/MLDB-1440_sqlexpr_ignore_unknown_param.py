#
# MLDB-1440_sqlexpr_ignore_unknown_param
# 2016-03-09
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1440Test(MldbUnitTest):  
    @classmethod
    def setUpClass(self):
        mldb.put("/v1/functions/noIgnore", {
            "type": "sql.expression",
            "params": {
                "expression": "a+b as rez",
                "prepared": True
            }
        })

    def test_noIgnore_with_known(self):
        self.assertTableResultEquals(
            mldb.query("select noIgnore({a:1, b:2}) as *"),
            [
                ["_rowName", "rez"],
                ["result",  3 ]
            ]
        )

    def test_noIgnore_with_unknown(self):
        self.assertTableResultEquals(
            mldb.query("select noIgnore({a:1, b:2, c:5}) as *"),
            [
                ["_rowName", "rez"],
                ["result",  3 ]
            ]
        )
    
    def test_noIgnore_with_unknown_get(self):
        mldb.get("/v1/functions/noIgnore/application", input={"a":1, "b": 2, "c": 5})

        self.assertTableResultEquals(
            mldb.query("select noIgnore({a:1, b:2, c:5}) as *"),
            [
                ["_rowName", "rez"],
                ["result",  3 ]
            ]
        )



mldb.run_tests()

