#
# MLDB-1950-crash-in-merge.py
# Guy Dumais, 2016-09-16
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1950CrashInMerge(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        ds = mldb.create_dataset({"id": "data",
                                  "type": "sparse.mutable"})
        for i in range(2):
            ds.record_row('rowname'+str(i), [['col', 1, 0]])

        ds.commit()


    @unittest.skip("awaiting fix to MLDB-1950")
    def test_merge_with_duplicate(self):
        mldb.query("""
            SELECT * from merge(
                (SELECT * NAMED 'name' FROM data),
                (SELECT * NAMED 'name' FROM data)
            )
        """)

if __name__ == '__main__':
    mldb.run_tests()
