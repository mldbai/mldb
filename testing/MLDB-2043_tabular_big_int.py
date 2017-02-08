#
# MLDB-2043_tabular_big_int.py
# Francois Maillet, 2016-11-02
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2043TabularBigInt(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        for t in ["sparse.mutable", "tabular"]:
            mldb.put('/v1/datasets/%s' % t, { "type":t })

            mldb.post('/v1/datasets/%s/rows' % t, {
                "rowName": "first row",
                "columns": [
                    ["first column", 9936626511, 0]
                ]
            })

            mldb.post('/v1/datasets/%s/rows' % t, {
                "rowName": "second row",
                "columns": [
                    ["first column", 0, 0]
                ]
            })
            mldb.post("/v1/datasets/%s/commit" % t)

    def test_it(self):
        for t in ["sparse.mutable", "tabular"]:
            self.assertTableResultEquals(
                        mldb.query("select * from \"%s\" order by rowName() ASC" % t),
                        [
                            [ "_rowName", "first column"],
                            ["first row",  9936626511 ],
                            ["second row",  0 ]
                        ]
                    )


if __name__ == '__main__':
    mldb.run_tests()
