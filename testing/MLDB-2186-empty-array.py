# MLDB-2186-empty-array.py
# Guy Dumais, 2017-04-17
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

import random
mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2186EmptyArray(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
  
        ds = mldb.create_dataset({ "id": "dataset", "type": "sparse.mutable" })
        ds.record_row("row2", [["text", "one,two", 0]])
        ds.record_row("row4", [["text", "", 0]])
        ds.commit()



    def test_confusion_empty_array_to_null(self):

        mldb.log(mldb.query("""
        SELECT sum({labels.* AS *}) FROM (
            SELECT tokenize(text, {splitChars:',',quoteChar:''}) AS labels FROM dataset
        )
        """))

if __name__ == '__main__':
    mldb.run_tests()
