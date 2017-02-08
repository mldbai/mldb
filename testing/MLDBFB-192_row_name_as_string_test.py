#
# MLDBFB-192_row_name_as_string_test.py
# Mich, 2016-02-02
# Copyright (c) 2016 mldb.ai inc. All rights reserved.
#
import tempfile
import os
import unittest
mldb = mldb_wrapper.wrap(mldb) # noqa

class RowNameAsStringTest(MldbUnitTest):
    query = "SELECT * FROM csv ORDER BY rowName()"

    @classmethod
    def setUpClass(cls):
        tmp_file = tempfile.NamedTemporaryFile(
            prefix=os.getcwd() + '/build/x86_64/tmp/')
        with open(tmp_file.name, 'wt') as f:
            f.write("header\n")
            f.write("val1\n")
            f.write("val2\n")

        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'file:///' + tmp_file.name,
                "outputDataset": {
                    "id": "csv",
                },
                "runOnCreation": True
            }
        }
        mldb.put("/v1/procedures/csv_proc", csv_conf)


    def test_flat_result(self):
        res = mldb.query(self.__class__.query)
        self.assertTableResultEquals(res, [
            ['_rowName', 'header'],
            ['2', 'val1'],
            ['3', 'val2']
        ])

    def test_object_result(self):
        res = mldb.get('/v1/query', q=self.__class__.query).json()
        # because of the now ts beign inserted we only check for row names
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]["rowName"], "2")
        self.assertEqual(res[1]["rowName"], "3")

class NullNameTest(MldbUnitTest):

    def test_record_nonw_row_name(self):
        ds = mldb.create_dataset({
            'id' : 'ds1',
            'type': 'sparse.mutable'
        })
        with self.assertRaises(Exception) as exc:
            ds.record_row(None, [['colA', 1, 1]])
        self.assertEqual(str(type(exc.exception)),
                         "<class 'Boost.Python.ArgumentError'>")

    def test_post_row_name_none(self):
        mldb.put('/v1/datasets/ds2', {
            'type' : 'sparse.mutable'
        })

        with self.assertRaises(mldb_wrapper.ResponseException): # noqa
            mldb.post('/v1/datasets/ds2/rows', {
                'rowName' : None, # should not work
                'columns' : [['colA', 1, 1]]
            })

    def test_post_no_row_name(self):
        mldb.put('/v1/datasets/ds3', {
            'type' : 'sparse.mutable'
        })

        with self.assertRaises(mldb_wrapper.ResponseException): # noqa
            mldb.post('/v1/datasets/ds3/rows', {
                # rowName missing -> should not work
                'columns' : [['colA', 1, 1]]
            })

    def test_record_empty_row_name(self):
        ds = mldb.create_dataset({
            'id' : 'ds4',
            'type': 'sparse.mutable'
        })
        ds.record_row("", [['colA', 1, 1]])
        ds.commit()
        res = mldb.query("SELECT * FROM ds4")
        self.assertEqual(res[1][0], '""')

    def test_record_null_row_name(self):
        mldb.put('/v1/datasets/ds_null', {'type' : 'sparse.mutable'})
        with self.assertRaises(mldb_wrapper.ResponseException): # noqa
            mldb.post('/v1/datasets/ds_null/rows',
                    {'rowName' : None, 'columns' : [['colA', 1, 1]]})

if __name__ == '__main__':
    mldb.run_tests()
