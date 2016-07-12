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
        ds.record_row("row_a",[["x", "toy story", 0],["y", "123456", 0]])
        ds.record_row("row_b",[["x", "terminator", 0]])
        ds.commit()

        ds = mldb.create_dataset({ "id": "dataset3", "type": "sparse.mutable" })
        ds.record_row("row_a",[["z", 0.1, 0]])
        ds.record_row("row_b",[["z", 0.2, 0]])
        ds.commit()

        ds = mldb.create_dataset({ "id": "dataset4", "type": "sparse.mutable" })
        ds.record_row("result",[["z", 0.1, 0]])
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

    def test_transpose_subselect(self):

        res = mldb.put('/v1/functions/bop', {
            'type': 'sql.query',
            'params': {
                'query': "select * from transpose((select $k as blah))"
            }
        })

        res = mldb.query("select bop({k : 'x'})")
        mldb.log(res)

        expected = [["_rowName", "bop({k : 'x'}).result"],["result","x"]]

        self.assertEqual(res, expected)
  
    def test_transpose_join(self):

        res = mldb.query("select * from transpose(dataset1 as t1 JOIN dataset2 as t2 on t1.x = t2.x)");

        mldb.log(res)

        res = mldb.put('/v1/functions/bop', {
            'type': 'sql.query',
            'params': {
                'query': "select * from transpose(dataset1 as t1 JOIN dataset2 as t2 on t1.x = t2.x)"
            }
        })

        res = mldb.put('/v1/functions/bop2', {
            'type': 'sql.query',
            'params': {
                'query': "select * from transpose(dataset1 as t1 JOIN dataset2 as t2 on t1.x = t2.x) offset 2"
            }
        })

        res = mldb.query("select bop()")
        mldb.log(res)

        expected = [["_rowName","bop().[a]-[row_a]"],["result","toy story"]]

        res = mldb.query("select bop2()")
        mldb.log(res)

        expected = [["_rowName","bop2().[a]-[row_a]"], ["result", "123456"]]

        self.assertEqual(res, expected)

    def test_transpose_join_cross(self):

        res = mldb.query("select * from transpose(dataset1 as t1 JOIN dataset2 as t2)");

        mldb.log(res)

        res = mldb.put('/v1/functions/bop', {
            'type': 'sql.query',
            'params': {
                'query': "select * from transpose(dataset1 as t1 JOIN dataset2 as t2)"
            }
        })      

        res = mldb.query("select bop()")
        mldb.log(res)

        expected =  [["_rowName", "bop().[a]-[row_a]", "bop().[a]-[row_b]"],
                     ["result", "toy story", "toy story"]]

        self.assertEqual(res, expected)

    def test_transpose_transpose(self):

        res = mldb.put('/v1/functions/bop', {
            'type': 'sql.query',
            'params': {
                'query': "select * from transpose(transpose(dataset2))"
            }
        })

        res = mldb.query("select bop()")
        mldb.log(res)

        expected = [["_rowName","bop().x"],["result","terminator"]]

        self.assertEqual(res, expected)

    def test_transpose_transpose(self):

        res = mldb.put('/v1/functions/bop', {
            'type': 'sql.query',
            'params': {
                'query': "select * from transpose(transpose(dataset2))"
            }
        })

        res = mldb.query("select bop()")
        mldb.log(res)

        expected = [["_rowName","bop().x"],["result","terminator"]]

        self.assertEqual(res, expected)

    def test_merge_dataset(self):

        res = mldb.put('/v1/functions/bop', {
            'type': 'sql.query',
            'params': {
                'query': "select * from merge(dataset2, dataset3)"
            }
        })

        res = mldb.query("select bop()")
        mldb.log(res)

        expected = [["_rowName","bop().x","bop().y","bop().z"],["result","toy story","123456",0.10000000149011612]]

        self.assertEqual(res, expected)

    def test_merge_subselect(self):

        res = mldb.put('/v1/functions/bop', {
            'type': 'sql.query',
            'params': {
                'query': "select * from merge(dataset4, (select $k as blah))"
            }
        })

        res = mldb.query("select bop({k : 'x'})")
        mldb.log(res)

        expected = [["_rowName","bop({k : 'x'}).blah","bop({k : 'x'}).z"],["result","x",0.10000000149011612]]

        self.assertEqual(res, expected)

    def test_row_dataset(self):

        res = mldb.put('/v1/functions/bop', {
            'type': 'sql.query',
            'params': {
                'query': "select * from row_dataset({x: 1, y:2, z: 'three'})"
            }
        })

        res = mldb.query("select bop()")
        mldb.log("here")
        mldb.log(res)

        expected = [["_rowName","bop().column","bop().value"],["result","x",1]]

        self.assertEqual(res, expected)

mldb.run_tests()