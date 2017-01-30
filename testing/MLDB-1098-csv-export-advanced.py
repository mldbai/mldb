
#
# MLDB-1098-csv-export-advanced.py
# Mich, 2015-11-18
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#

import tempfile
import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

tmp_file = tempfile.NamedTemporaryFile(dir='build/x86_64/tmp')


class CsvExportAdvancedTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        mldb.put('/v1/datasets/myDataset', {
            'type' : 'sparse.mutable'
        })

        def record_row(row_name, columns):
            mldb.post('/v1/datasets/myDataset/rows', {
                'rowName' : row_name,
                'columns' : columns
            })

        record_row(1, [['A', 'A1', 0]])
        record_row(2, [['B', 'B2', 0]])
        record_row(3, [['C', 'C3', 0]])
        record_row(4, [['A', 'A4', 0], ['C', 'C4', 0]])

        mldb.post('/v1/datasets/myDataset/commit', {})

    def log_tmp_file(self):
        f = open(tmp_file.name, 'rt')
        for line in f.readlines():
            mldb.log(line[:-1])

    def assert_file_content(self, lines_expect):
        f = open(tmp_file.name, 'rt')
        for index, expect in enumerate(lines_expect):
            line = f.readline()[:-1]
            self.assertEqual(line, expect)

    def test_select_star(self):
        mldb.put('/v1/procedures/export', {
            'type' : 'export.csv',
            'params' : {
                'exportData' : 'select * from myDataset order by rowName()',
                'dataFileUrl' : 'file://' + tmp_file.name
            }
        })

        mldb.post('/v1/procedures/export/runs')

        lines_expect = ['A,B,C',
                        'A1,,',
                        ',B2,',
                        ',,C3',
                        'A4,,C4']
        self.assert_file_content(lines_expect)

    def test_select_row_name_and_star(self):
        mldb.put('/v1/procedures/export', {
            'type' : 'export.csv',
            'params' : {
                'exportData' :
                    'select rowName() as rowName, * from myDataset '
                    'order by rowName()',
                'dataFileUrl' : 'file://' + tmp_file.name
            }
        })

        mldb.post('/v1/procedures/export/runs', {})

        lines_expect = ['rowName,A,B,C',
                        '1,A1,,',
                        '2,,B2,',
                        '3,,,C3',
                        '4,A4,,C4']
        self.assert_file_content(lines_expect)

    def select_dual_col_test(self):
        mldb.put('/v1/procedures/export', {
            'type' : 'export.csv',
            'params' : {
                'exportData' :
                    "select B, B, B as D from myDataset where B = 'B2'",
                'dataFileUrl' : 'file://' + tmp_file.name
            }
        })

        mldb.post('/v1/procedures/export/runs', {})

        lines_expect = ['B,B,D',
                        'B2,B2,B2']
        self.assert_file_content(lines_expect)

    def test_select_mixed_up(self):
        mldb.put('/v1/procedures/export', {
            'type' : 'export.csv',
            'params' : {
                'exportData' :
                    "select 'foo' as foo, bar, rowName() as rowName, *, B, "
                    "B as D from myDataset order by rowName()",
                'dataFileUrl' : 'file://' + tmp_file.name
            }
        })

        mldb.post('/v1/procedures/export/runs')
        self.log_tmp_file()
        lines_expect = ['foo,bar,rowName,A,B,C,B,D',
                        'foo,,1,A1,,,,',
                        'foo,,2,,B2,,B2,B2',
                        'foo,,3,,,C3,,',
                        'foo,,4,A4,,C4,,']
        self.assert_file_content(lines_expect)


if __name__ == '__main__':
    mldb.run_tests()
