#
# MLDB-2103-merge-row-dataset.py
# Mathieu marquis Bolduc, 2017-01-06
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2103MergeRowDataset(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        pass

    def test_it(self):
        self.assertTableResultEquals(
            mldb.query("SELECT * FROM merge(row_dataset([0, 0]), row_dataset([1, 1]))"),
            [[ "_rowName", "column", "value"],
             [ "0", "0", 1 ],
             [ "1", "1", 1 ]])       

if __name__ == '__main__':
    mldb.run_tests()
