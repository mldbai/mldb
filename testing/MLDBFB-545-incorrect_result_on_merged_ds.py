#
# MLDBFB-545-incorrect_result_on_merged_ds.py
# Mich, 2016-05-27
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class Mldbfb545MergeDsWhereQueryTest(MldbUnitTest):  # noqa

    def test_MLDBFB_545_where_query_beh(self):
        self.run_MLDBFB_545_with_ds_type('beh')

    def test_MLDBFB_545_where_query_sparse(self):
        self.run_MLDBFB_545_with_ds_type('sparse')

    def run_MLDBFB_545_with_ds_type(self, ds_type):
        id1 = ds_type + 'mldbfb545_1'
        ds = mldb.create_dataset({
            'id' : id1,
            'type': ds_type + '.mutable'
        })
        ds.record_row('user1', [['converted', 'n', 0]])
        ds.commit()

        id2 = ds_type + 'mldbfb545_2'
        ds = mldb.create_dataset({
            'id' : id2,
            'type' : ds_type + '.mutable'
        })
        ds.record_row('user2', [['blah', 'blah', 0]])
        ds.commit()

        # query directly on the dataset works
        res = mldb.query("""
            SELECT * FROM {} WHERE converted='c' LIMIT 1
        """.format(id1))
        self.assertEqual(len(res), 1)

        merge_id = ds_type + 'mldbfb545merged'
        mldb.put("/v1/datasets/" + merge_id, {
            "type": "merged",
            "params": {
                "datasets": [{"id": id1}, {"id": id2}]
            }
        })

        # query on the merged dataset yields incorrect results
        res = mldb.query("""
            SELECT * FROM {} WHERE converted='c' LIMIT 1
        """.format(merge_id))
        mldb.log(res)
        self.assertEqual(len(res), 1)

    def test_merge_freeze_beh(self):
        self.run_freeze_with_ds_type('beh')

    def test_merge_freeze_sparse(self):
        self.run_freeze_with_ds_type('sparse')

    def run_freeze_with_ds_type(self, ds_type):
        ds = mldb.create_dataset({
            'id' : ds_type,
            'type': ds_type + '.mutable'
        })
        ds.record_row('user1', [['converted', 'n', 0]])
        ds.commit()

        ds = mldb.create_dataset({
            'id' : ds_type + "2",
            'type' : ds_type + '.mutable'
        })
        ds.record_row('user2', [['converted', 'n', 0]])
        ds.commit()

        # query directly on the dataset works
        res = mldb.query("""
            SELECT * FROM {} WHERE converted='c' LIMIT 1
        """.format(ds_type))
        self.assertEqual(len(res), 1)

        mldb.put("/v1/datasets/mergedDs" + ds_type, {
            "type": "merged",
            "params": {
                "datasets": [{"id": ds_type}, {"id": ds_type + "2"}]
            }
        })

        # query on the merged dataset yields incorrect results
        res = mldb.query("""
            SELECT * FROM mergedDs{} WHERE converted='c' LIMIT 1
        """.format(ds_type))
        mldb.log(res)
        self.assertEqual(len(res), 1)

if __name__ == '__main__':
    mldb.run_tests()
