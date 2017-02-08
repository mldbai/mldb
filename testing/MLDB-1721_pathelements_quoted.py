#
# MLDB-1721_pathelements_quoted.py
# Francois Maillet, 11 juin 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1721(MldbUnitTest):  
    @classmethod
    def setUpClass(self):
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        ds.record_row("a",[["text", "hola \nreturn", 0]])
        ds.record_row("b",[["text", "hola \\return", 0]])
        ds.commit()

    def test_select_after_token(self):
        print mldb.post('/v1/procedures', {
            'type': 'transform',
            'params': {
                'inputData': """
                    SELECT
                        tokenize(
                            lower(text),
                            {splitChars: ' -''"?!;:/[]*,.'}
                        ) as *
                    FROM sample
                    """,
                'outputDataset': 'bag_of_words',
                'runOnCreation': True
            }
        })

        self.assertTableResultEquals(
            mldb.query("select * from bag_of_words order by rowName() DESC"),
                [
                    [
                        "_rowName",
                        "\\return",
                        "hola",
                        "\"\nreturn\""
                    ],
                    ["b", 1, 1, None],
                    ["a", None, 1, 1]
                ])

mldb.run_tests()

