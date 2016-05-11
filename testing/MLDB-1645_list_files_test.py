#
# MLDB-1645_list_files_test.py
# Mich, 2016-05-11
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb) # noqa

class ListFilesTest(MldbUnitTest):  # noqa

    def test_it(self):
        mldb.post('/v1/procedures', {
            'type' : 'list.files',
            'params' : {
                'path' : 'file://./mldb/testing/',
                'outputDataset' : {
                    'id' : 'output',
                    'type' : 'tabular'
                },
                'runOnCreation' : True
            }
        })

        res = mldb.query("""
            SELECT * FROM output WHERE
            rowName()='file://./mldb/testing/MLDB-1645_list_files_test.py'
        """)
        self.assertEqual(len(res), 2)
        self.assertEqual(
            res[1][0], '"file://./mldb/testing/MLDB-1645_list_files_test.py"')

if __name__ == '__main__':
    mldb.run_tests()
