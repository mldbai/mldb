#
# MLDB-2126-export-structured.py
# Mathieu Marquis Bolduc, 2017-01-25
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

import tempfile
import codecs
import os

from mldb import mldb, MldbUnitTest, ResponseException

tmp_dir = os.getenv('TMP')

class MLDB2126exportstructuredTest(MldbUnitTest):  # noqa

    def assert_file_content(self, filename, lines_expect):
        f = codecs.open(filename, 'rb', 'utf8')
        for index, expect in enumerate(lines_expect):
            line = f.readline()[:-1]
            self.assertEqual(line, expect)

    def test_row(self):
        # create the dataset
        mldb.put('/v1/datasets/patate', {
            'type': 'tabular'
        })
        mldb.post('/v1/datasets/patate/rows', {
            'rowName': 0,
            'columns': [
                ['x.a', 1, 0],
                ['x.b', 2, 0]
            ]}
        )
        mldb.post('/v1/datasets/patate/commit')

        tmp_file = tempfile.NamedTemporaryFile(dir=tmp_dir)

        res = mldb.post('/v1/procedures', {
            'type': 'export.csv',
            'params': {
                'exportData': 'select x as x from patate',
                'dataFileUrl': 'file://' + tmp_file.name,
            }
        })

        mldb.log(res)

        lines_expect = ['x.a,x.b',
                        '1,2'
                        ]
        self.assert_file_content(tmp_file.name, lines_expect)

if __name__ == '__main__':
    mldb.run_tests()
