# -*- coding: utf-8 -*-
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Utf8Test(MldbUnitTest):  

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "example", "type": "sparse.mutable" })
        ds.record_row("a",[["x", 1, 0]])
        ds.record_row("r1", [ ["ê", 1, 0], ["b", 2, 0] ])
        ds.record_row("r2", [ ["a", 3, 0], ["b", 4, 0] ])
        ds.record_row("rñ3",[ ["a", 5, 0], ["b", 6, 0] ])
        ds.record_row("r4", [ ["a", 7, 0], ["b", 8, 0] ])
      
        ds.commit()

    def test_unicode_in_select(self):
        rez = mldb.put('/v1/procedures/transform', {
            "type": "transform",
            "params": {
                "inputData": "SELECT 'françois', x FROM example",
                "outputDataset": "output",
                "runOnCreation": True
            }
        })
        #mldb.log(rez.json())

    def test_unicode_in_column_name(self):
        rez = mldb.put("/v1/procedures/x", {
            "type": "transform",
            "params": {
                "inputData": "select * from example", 
                "outputDataset": "ex2",
                "runOnCreation": True
            }
        })
        #mldb.log(rez.json())

mldb.run_tests()

