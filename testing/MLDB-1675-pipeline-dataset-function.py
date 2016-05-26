# MLDB-1639-join-where.py
# Mathieu Marquis Bolduc, 2016-16-05
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest
import json

mldb = mldb_wrapper.wrap(mldb) # noqa

class DatasetFunctionTest(MldbUnitTest):

    @classmethod
    def setUpClass(self):
        # create a dummy dataset
        ds = mldb.create_dataset({ "id": "dataset1", "type": "sparse.mutable" })
        ds.record_row("a",[["x", "toy story", 0]])
        ds.commit()
        
        ds = mldb.create_dataset({ "id": "dataset2", "type": "sparse.mutable" })
        ds.record_row("row_a",[["x", "toy story", 0]])
        ds.record_row("row_b",[["x", "terminator", 0]])
        ds.commit()

    def test_transpose_dataset(self):

        res = mldb.put('/v1/functions/bop', {
            'type': 'sql.query',
            'params': {
                'query': "select * from transpose(dataset2)"
            }
        })

        res = mldb.query("select bop()")
        mldb.log(res)

        expected = [["_rowName","bop().row_a","bop().row_b"],["result","toy story","terminator"]]

        self.assertEqual(res, expected)

    @unittest.expectedFailure #not the expected rowname
    def test_transpose_subselect(self):

        res = mldb.put('/v1/functions/bop', {
            'type': 'sql.query',
            'params': {
                'query': "select * from transpose((select $k as blah))"
            }
        })

        mldb.log(res)

        res = mldb.query("select bop({k : 'x'})")
        mldb.log(res)

        expected = [["_rowName", "bop().result"],["blah","x"]]

        self.assertEqual(res, expected)

    #joins executors dont support taking columns yet. Is there a way not to keep the whole transposed table in memory?
    #one way or another we have to do the full join first to find all the rows
    #then we either keep the rows or get them again
    #except in the pipeline executor we dont have direct access to row, only iterative

    #in a join dataset, a column belongs to one or the other...
    #can we take all columns from one dataset, then the other?

    #should we have a transpose-join-executor?

    #if no subselect is inside the transpose, its probably better to use the old pipeline...
    @unittest.expectedFailure  
    def test_transpose_join(self):

        res = mldb.put('/v1/functions/bop', {
            'type': 'sql.query',
            'params': {
                'query': "select * from transpose(dataset1 as t1 JOIN dataset2 as t2 on t1.x = t2.x)"
            }
        })

        mldb.log(res)

        res = mldb.query("select bop()")
        mldb.log(res)

        expected = [["_rowName", "bop().result"],["blah","x"]]

        self.assertEqual(res, expected)

mldb.run_tests()