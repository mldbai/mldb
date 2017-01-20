#
# MLDB-1452-like-operator
# 2016-03-16
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

class LikeTest(MldbUnitTest):  # noqa

    def test_like_select(self):

        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        ds.record_row("a",[["x", "acrasial", 0]])
        ds.record_row("b",[["x", "blaternation", 0]])
        ds.record_row("c",[["x", "citharize", 0]])
        ds.record_row("d",[["x", "drollic", 0]])
        ds.record_row("e",[["x", "egrote", 0]])
        ds.commit()

        res = mldb.query('''
            select x LIKE '%' as v
            from sample
            order by rowPath()
        ''')

        expected = [["_rowName","v"],["a",1],["b",1],["c",1],["d",1],["e",1],]
        self.assertEqual(res, expected)

        res = mldb.query('''
            select x LIKE '%o%' as v
            from sample
            order by rowPath()
        ''')

        expected = [["_rowName","v"],["a",0],["b",1],["c",0],["d",1],["e",1]]
        self.assertEqual(res, expected)

        res = mldb.query('''
            select x NOT LIKE '%o%' as v
            from sample
            order by rowPath()
        ''')

        expected = [["_rowName","v"],["a",1],["b",0],["c",1],["d",0],["e",0]]
        self.assertEqual(res, expected)

        res = mldb.query('''
            select x LIKE '______' as v
            from sample
            order by rowPath()
        ''')

        expected = [["_rowName","v"],["a",0],["b",0],["c",0],["d",0],["e",1]]
        self.assertEqual(res, expected)

        res = mldb.query('''
            select x LIKE '___ll__' as v
            from sample
            order by rowPath()
        ''')

        expected = [["_rowName","v"],["a",0],["b",0],["c",0],["d",1],["e",0]]
        self.assertEqual(res, expected)

        res = mldb.query('''
            select x LIKE '%t_' as v
            from sample
            order by rowPath()
        ''')

        expected = [["_rowName","v"],["a",0],["b",0],["c",0],["d",0],["e",1]]
        self.assertEqual(res, expected)

    def test_like_in_where(self):

        ds = mldb.create_dataset({ "id": "sample2", "type": "sparse.mutable" })
        ds.record_row("a",[["x", "acrasial", 0]])
        ds.record_row("b",[["x", "blaternation", 0]])
        ds.record_row("c",[["x", "citharize", 0]])
        ds.record_row("d",[["x", "drollic", 0]])
        ds.record_row("e",[["x", "egrote", 0]])
        ds.commit()

        res = mldb.query('''
            select x
            from sample2
            where x LIKE '%o%'
            order by rowPath()
        ''')

        expected = [["_rowName","x"],["b","blaternation"],["d","drollic"],["e","egrote"]]
        self.assertEqual(res, expected)

    def test_like_special(self):

        ds = mldb.create_dataset({ "id": "sample3", "type": "sparse.mutable" })
        ds.record_row("a",[["x", "acra[sial", 0]])
        ds.record_row("b",[["x", "blate*rnation", 0]])
        ds.record_row("c",[["x", "cit.harize", 0]])
        ds.record_row("d",[["x", "dro|llic", 0]])
        ds.record_row("e",[["x", "eg(ro)te", 0]])
        ds.record_row("f",[["x", "famelico$e", 0]])
        ds.record_row("g",[["x", "gardev^iance", 0]])
        ds.commit()

        res = mldb.query('''
            select x
            from sample3
            where x LIKE '%[____'
        ''')

        expected = [["_rowName","x"],["a","acra[sial"]]
        self.assertEqual(res, expected)

        res = mldb.query('''
            select x
            from sample3
            where x LIKE '%*%'
        ''')


        expected = [["_rowName","x"],["b","blate*rnation"]]
        self.assertEqual(res, expected)

        res = mldb.query('''
            select x
            from sample3
            where x LIKE '___.%'
        ''')

        expected = [["_rowName","x"],["c","cit.harize"]]
        self.assertEqual(res, expected)

        res = mldb.query('''
            select x
            from sample3
            where x LIKE '__o|ll_%'
        ''')

        expected = [["_rowName","x"],["d","dro|llic"]]
        self.assertEqual(res, expected)

        res = mldb.query('''
            select x
            from sample3
            where x LIKE '%(__)%'
        ''')

        expected = [["_rowName","x"],["e","eg(ro)te"]]
        self.assertEqual(res, expected)

        res = mldb.query('''
            select x
            from sample3
            where x LIKE '%$%'
        ''')

        expected = [["_rowName","x"],["f","famelico$e"]]
        self.assertEqual(res, expected)

        res = mldb.query('''
            select x
            from sample3
            where x LIKE '%^%'
        ''')

        expected = [["_rowName","x"],["g","gardev^iance"]]
        self.assertEqual(res, expected)

    def test_like_number(self):

        ds = mldb.create_dataset({ "id": "sample4", "type": "sparse.mutable" })
        ds.record_row("a",[["x", 0, 0]])
        ds.record_row("b",[["x", 12345, 0]])
        ds.record_row("c",[["x", 12345.00, 0]])
        ds.commit()

        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            res = mldb.query('''
                select x
                from sample4
                where x LIKE '12345%'
            ''')

    def test_like_from_dataset(self):

        ds = mldb.create_dataset({ "id": "sample5", "type": "sparse.mutable" })
        ds.record_row("a",[["x", "hyometer", 0], ["y", "hyo%", 0]])
        ds.record_row("b",[["x", "ichthyarchy", 0], ["y", "forgetit", 0]])
        ds.commit()

        res = mldb.query('''
            select x
            from sample5
            where x LIKE y
        ''')

        expected = [["_rowName","x"],["a","hyometer"]]
        self.assertEqual(res, expected)

    def test_null_like(self):
        """
        MLDB-1727 test null like
        """
        res = mldb.get('/v1/query',
                       q="SELECT NULL LIKE 'abc' AS res NAMED 'row'")
        self.assertFullResultEquals(res.json(), [{
            'rowName' : "row",
            'columns' : [['res', None, "-Inf"]]
        }])

    def test_like_null(self):
        """
        MLDB-1727 test like null
        """
        res = mldb.get('/v1/query',
                       q="SELECT 'abc' LIKE NULL AS res NAMED 'row'")
        self.assertFullResultEquals(res.json(), [{
            'rowName' : "row",
            'columns' : [['res', None, "-Inf"]]
        }])

    def test_join_no_on_clause(self):
        """
        MLDB-1617
        """
        res1 = mldb.query("select 'apple' like ('%'+'p'+'%')")
        mldb.log(res1)

        res2 = mldb.query("select 'apple' like '%'+'p'+'%'")
        mldb.log(res2)

        self.assertEqual(res1[1], res2[1]);


mldb.run_tests()
