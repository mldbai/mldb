#
# MLDB-1639-join-where.py
# Mathieu Marquis Bolduc, 2016-16-05
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class SampleTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "test1", "type": "sparse.mutable" })
        ds.record_row("a",[["x", "1", 0]])
        ds.record_row("b",[["x", "2", 0]])
        ds.commit()
        
        ds = mldb.create_dataset({ "id": "test2", "type": "sparse.mutable" })
        ds.record_row("a",[["x", "2", 0]])
        ds.record_row("b",[["x", "3", 0]])
        ds.commit()
        
    def test_join_no_on_clause(self):

        res = mldb.query("select * from test1 join test2")
        mldb.log(res)
       
        res = mldb.query("select * from test1 join test2 where test1.x = text2.x")
        mldb.log(res)
       

mldb.run_tests()
