# MLDB-1841-distinct-on.py
# Mathieu Marquis Bolduc, 2016-07-21
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
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

    def test_distincton(self):
        res = mldb.query("SELECT DISTINCT ON (x) x, y FROM dataset1 ORDER BY x,y")

        expected = [["_rowName","x","y"],
                    ["row1", 1,  1 ],
                    ["row2", 2,  2 ]]

        self.assertEqual(res, expected)
   
    def test_order(self):
        res = mldb.query("""
                SELECT DISTINCT ON (x) x, z
                FROM dataset1 
                ORDER BY x,y DESC""")
        expected = [["_rowName","x","z"],
                    ["row4",1,2],
                    ["row5",2,3]]

        self.assertEqual(res, expected)
    
    def test_distincton_where(self):

        res = mldb.query("SELECT DISTINCT ON (x) x, y FROM dataset1 WHERE y % 2 = 0 ORDER BY x,y")

        expected = [["_rowName", "x", "y"],
                    ["row4", 1, 4],
                    ["row2", 2, 2]]

        self.assertEqual(res, expected)

    def test_distincton_offset(self):

        #without offset/limit we have rows 1, 3, 5
        res = mldb.query("SELECT DISTINCT ON (z) x, y FROM dataset1 ORDER BY z OFFSET 1")

        expected = [["_rowName", "x", "y"],
                    ["row3", 1, 3],
                    ["row5", 2, 5]]

        self.assertEqual(res, expected)

    def test_distincton_limit(self):

        #without offset/limit we have rows 1, 3, 5
        res = mldb.query("SELECT DISTINCT ON (z) x, y FROM dataset1 ORDER BY z LIMIT 2")

        expected = [["_rowName", "x", "y"],
                    ["row1", 1, 1],
                    ["row3", 1, 3]]

        self.assertEqual(res, expected)

    def test_distincton_limit_offset(self):

        #without offset/limit we have rows 1, 3, 5
        res = mldb.query("SELECT DISTINCT ON (z) x, y FROM dataset1 ORDER BY z LIMIT 1 OFFSET 1")

        expected = [["_rowName", "x", "y"],
                    ["row3", 1, 3]]

        self.assertEqual(res, expected)   

    def test_distincton_groupby(self):

        res = mldb.query("SELECT DISTINCT ON (max(x)) z, max(x) FROM dataset1 GROUP BY z ORDER BY max(x)")

        expected = [["_rowName", "max(x)", "z"],
                    ["[2]", 1, 2],
                    ["[1]", 2, 1]]

        self.assertEqual(res, expected)

    def test_distincton_groupby_offset(self):

        res = mldb.query("SELECT DISTINCT ON (max(x)) z, max(x) FROM dataset1 GROUP BY z ORDER BY max(x) OFFSET 1")
        expected = [["_rowName", "max(x)", "z"],
                    ["[1]", 2, 1]]

        self.assertEqual(res, expected)

    def test_distincton_groupby_limit(self):

        res = mldb.query("SELECT DISTINCT ON (max(x)) z, max(x) FROM dataset1 GROUP BY z ORDER BY max(x) LIMIT 1")
        expected = [["_rowName", "max(x)", "z"],
                    ["[2]", 1, 2]]

        self.assertEqual(res, expected)

    def test_distincton_groupby_limit_offset(self):

        res = mldb.query("SELECT DISTINCT ON (max(x)) z, max(x) FROM dataset1 GROUP BY z ORDER BY max(x) LIMIT 1 OFFSET 1")
        expected = [["_rowName", "max(x)", "z"],
                    ["[1]", 2, 1]]

        self.assertEqual(res, expected)

    def test_distincton_pipeline(self):

        res = mldb.put('/v1/functions/mydistinct', {
                    'type': 'sql.query',
                    'params': {
                    'query': 'SELECT DISTINCT ON (x) x, y FROM dataset1 ORDER BY x,y'
                    }})

        res = mldb.query('SELECT mydistinct() as *')

        expected = [["_rowName","x","y"],
                    ["result", 1,  1 ]]

        self.assertEqual(res, expected)

    def test_distincton_where_pipeline(self):

        res = mldb.put('/v1/functions/mydistinct', {
                    'type': 'sql.query',
                    'params': {
                    'query': 'SELECT DISTINCT ON (x) x, y FROM dataset1 WHERE y % 2 = 0 ORDER BY x,y'
                    }})

        res = mldb.query('SELECT mydistinct() as *')

        expected = [["_rowName", "x", "y"],
                    ["result", 1, 4]]

        self.assertEqual(res, expected)

    def test_distincton_subselect(self):

        res = mldb.query("SELECT * FROM (SELECT DISTINCT ON (x) x, y FROM dataset1 ORDER BY x,y)")

        expected = [["_rowName","x","y"],
                    ["row1", 1,  1 ],
                    ["row2", 2,  2 ]]

        self.assertEqual(res, expected)

    def test_distincton_where_subselect(self):

        res = mldb.query("SELECT * FROM (SELECT DISTINCT ON (x) x, y FROM dataset1 WHERE y % 2 = 0 ORDER BY x,y)")

        expected = [["_rowName","x","y"],
                    ["row4", 1, 4],
                    ["row2", 2, 2]]

        self.assertEqual(res, expected)

    def test_distinct_parens(self):
       
        with self.assertMldbRaises(status_code=400):
            res = mldb.query("SELECT DISTINCT ON x FROM dataset1 ORDER BY x")

    def test_distinct_generic(self):
       
        with self.assertMldbRaises(expected_regexp="Generic 'DISTINCT' is not currently supported. Please use 'DISTINCT ON'."):
            res = mldb.query("SELECT DISTINCT x FROM dataset1")

    def test_distincton_multiple(self):

        res = mldb.query("SELECT DISTINCT ON (x,z) x, z FROM dataset1 ORDER BY x,z")

        expected = [["_rowName", "x", "z"],
                    ["row1", 1, 1],
                    ["row3", 1, 2],
                    ["row2", 2, 1],
                    ["row5", 2, 3]]

        self.assertEqual(res, expected)

    def test_distincton_groupby_multiple(self):

        res = mldb.query("SELECT DISTINCT ON (max(x),z) max(x), z FROM dataset1 GROUP BY z ORDER BY max(x),z")

        expected = [["_rowName","max(x)","z"],
                    ["[2]",1, 2],
                    ["[1]",2, 1],
                    ["[3]",2, 3]]

        self.assertEqual(res, expected)
   
    def test_distincton_transform(self):

        mldb.post('/v1/procedures', {
            'type': 'transform',
            'params': {
                'inputData': "SELECT DISTINCT ON (x,z) x, z FROM dataset1 ORDER BY x,z",
                'outputDataset': 'transformed',
                'runOnCreation': True
            }
        })

        res = mldb.query("SELECT * from transformed order by rowPath()")

        expected = [["_rowName", "x", "z"],
                    ["row1", 1, 1],
                    ["row2", 2, 1],
                    ["row3", 1, 2],
                    ["row5", 2, 3]]

        self.assertEqual(res, expected)


mldb.run_tests()
