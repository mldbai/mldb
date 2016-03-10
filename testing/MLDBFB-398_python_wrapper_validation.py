#
# MLDBFB-398_python_wrapper_validation.py
# 2016-03-10
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class PythonWrapperInvalidArgumentTest(MldbUnitTest):  

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        ds.record_row("a",[["x", 1, 0]])
        ds.commit()
        
    def test_put_arg_validation(self):
        mldb.put("/v1/procedures/transform_procedure", [], {
            "type": "transform",
            "params": {
                "inputData": {
                    "from" : "sample",
                    "where": "rowName() = '2'"
                },
                "outputDataset": "dataset2",
                "runOnCreation" : True
            }
        })

mldb.run_tests()


