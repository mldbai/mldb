#
# MLDB-1507-groupby.py
# 2016-04-07
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#


import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1507Test(MldbUnitTest):  

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
                    {splitchars: ' .()"', quotechar:''}
                )
            ) as *
            named 'counts'
            from %s
        ))
        order by counts desc limit 20
        """
        for s in ["sum", "count"]:
            self.assertTableResultEquals(
                mldb.query(q % (s, "titanic_tabular")),
                mldb.query(q % (s, "titanic_sparse"))
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

mldb.run_tests()


