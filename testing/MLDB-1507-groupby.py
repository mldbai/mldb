#
# MLDB-1507-groupby.py
# 2016-04-07
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1507Test(MldbUnitTest):  # noqa

    def test_group_count(self):
        mldb.put('/v1/procedures/import_titanic', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "http://public.mldb.ai/titanic_train.csv",
                "outputDataset": {"id":"titanic_tabular", "type":"tabular"},
                "runOnCreation": True
            }
        })

        mldb.put('/v1/procedures/import_titanic', {
            "type": "import.text",
            "params": {
                "dataFileUrl": "http://public.mldb.ai/titanic_train.csv",
                "outputDataset": {"id":"titanic_sparse", "type":"sparse.mutable"},
                "runOnCreation": True
            }
        })

        # basic equality assertion
        q = """
        select PassengerId, label, Pclass, Name, Sex, SibSp,
        Parch, Ticket, Fare, Embarked, Age, Cabin from %s order by rowName()
        """
        self.assertTableResultEquals(
            mldb.query(q % "titanic_tabular"),
            mldb.query(q % "titanic_sparse")
        )

        # this one of the demo queries
        q = """
        select * from transpose((
            select %s(
                tokenize(
                    jseval(
                        'return Name.replace(/([A-Z])/g, function(m, p) { return " "+p; });',
                        'Name', Name
                    ),
                    {splitChars: ' .()"', quoteChar:''}
                )
            ) as *
            named 'counts'
            from %s
        ))
        order by counts desc, rowHash()
        limit 20
        """
        for s in ["sum", "count"]:

            res1 = mldb.query(q % (s, "titanic_tabular"));
            res2 = mldb.query(q % (s, "titanic_sparse"));

            self.assertTableResultEquals(
                res1,
                res2
            )

        # all of these permutations should be equivalent!
        queries = ["count(*)", "count(Age)", "sum(Age)",
            "max(Age)", "count({Age, Sex})", "count({*})", "max({*})"]
        groups = [ "", "group by Pclass", "group by 1", "group by Sex"]
        for q in queries:
            for g in groups:
                print q, g
                self.assertTableResultEquals(
                    mldb.query("select %s from %s %s" % (q, "titanic_tabular", g)),
                    mldb.query("select %s from %s %s" % (q, "titanic_sparse", g)),
                )

    def test_groupby_select_star(self):
        ds = mldb.create_dataset({
            'id' : 'test_groupby_select_star',
            'type' : 'sparse.mutable'
        })
        ds.record_row('row1', [['colA', 1, 0]])
        ds.commit()

        msg = "Wildcard cannot be used with GROUP BY"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.query("SELECT * FROM test_groupby_select_star GROUP BY colA")


mldb.run_tests()
