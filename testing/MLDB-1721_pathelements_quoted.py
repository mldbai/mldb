#
# MLDB-1721_pathelements_quoted.py
# 11 juin 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1721(MldbUnitTest):  
    @classmethod
    def setUpClass(self):
        ds = mldb.create_dataset({ "id": "sample", "type": "sparse.mutable" })
        ds.record_row("a",[["text", "hola \nreturn", 0]])
        ds.commit()
        
    def test_select_after_token(self):
        print mldb.post('/v1/procedures', {
            'type': 'transform',
            'params': {
                'inputData': """
                    SELECT
                        tokenize(
                            lower(text),
                            {splitchars: ' -''"?!;:/[]*,.'}
                        ) as *
                    FROM sample
                    """,
                'outputDataset': 'bag_of_words',
                'runOnCreation': True
            }
        })

        mldb.query("select * from bag_of_words where rowName() = 'b'")

mldb.run_tests()

