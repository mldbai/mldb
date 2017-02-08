#
# MLDB-1899-duplicated-rows-in-equijoins.py
# Guy Dumais, 2016-08-17
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1899DuplicatedRowsInEquijoins(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        left_table = mldb.create_dataset({ "id": "left_table", "type": "sparse.mutable" })
        for index in range(100):
            left_table.record_row(index ,[["c", index % 10, 0], ["const", 2, 0]])
        left_table.commit()

        right_table = mldb.create_dataset({ "id": "right_table", "type": "sparse.mutable" })
        for index in range(100):
            right_table.record_row(index ,[["c", index % 10, 0], ["d", index % 2, 0]])
        right_table.commit()

    def test_no_duplicate_rows_in_left_join_with_batch_exec(self):
        # the left condition is always true
        resp = mldb.query("""
            SELECT count(*) FROM left_table LEFT JOIN right_table 
                            ON left_table.c = right_table.c
        """)

        self.assertEqual(resp[1][1], 1000, "expected 1000 rows to be returned")

        # the right condition is always false     
        resp = mldb.query("""
            SELECT count(*) FROM left_table LEFT JOIN right_table 
                            ON left_table.c = right_table.c AND
                            2 < right_table.d
        """)
        
        self.assertEqual(resp[1][1], 100, "expected 100 rows to be returned")

        # the right condition is half the time true
        resp = mldb.query("""
            SELECT count(*) FROM left_table LEFT JOIN right_table 
                            ON left_table.c = right_table.c AND
                            right_table.d = 1
        """)
        
        # when the row index is even the condition always fails. This accounts for
        # 50 rows.  When the index is odd, each of the left row match 10 different
        # right rows.  So this account for 50 * 10 rows.
        self.assertEqual(resp[1][1], 550, "expected 550 rows to be returned")
        
    def test_no_duplicate_rows_in_left_join_with_pipeline_exec(self):
        # the cross condition is always true
        resp = mldb.query("""
            SELECT count(*) FROM left_table LEFT JOIN right_table 
                            ON left_table.c = right_table.c AND
                            left_table.const > right_table.d
        """)

        self.assertEqual(resp[1][1], 1000, "expected 1000 rows to be returned")

        # the cross condition is always false     
        resp = mldb.query("""
            SELECT count(*) FROM left_table LEFT JOIN right_table 
                            ON left_table.c = right_table.c AND
                            left_table.const < right_table.d
        """)
        
        self.assertEqual(resp[1][1], 100, "expected 100 rows to be returned")

        # the right condition is half the time true, the cross condition is always true
        resp = mldb.query("""
            SELECT count(*) FROM left_table LEFT JOIN right_table 
                            ON left_table.c = right_table.c AND
                            left_table.const > right_table.d AND
                            right_table.d = 1 order by rowName()
        """)
        
        #mldb.log(resp)
        self.assertEqual(resp[1][1], 550, "expected 550 rows to be returned")

if __name__ == '__main__':
    mldb.run_tests()
