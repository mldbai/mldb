#
# MLDB-2065-transpose_rowdataset_segfaults.py
# Francois Maillet, 2016-11-29
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

from mldb import mldb, MldbUnitTest, ResponseException

class MLDB2065TransposeRowdatasetSegfaults(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        pass

    def test_it(self):
        self.assertTableResultEquals(
            mldb.query("SELECT * FROM row_dataset([0, 0])"),
            [
                ["_rowName","column","value"],
                ["0","0",0],
                ["1","1",0]
            ])

        # this segfault
        mldb.query("""
            SELECT * FROM transpose((row_dataset([0, 0])))
        """)

if __name__ == '__main__':
    request.set_return(mldb.run_tests())
