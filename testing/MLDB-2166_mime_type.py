#
# MLDB-2166_mime_type.py
# Francois Maillet, 2017-03-10
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2166MimeType(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        pass

    def test_it(self):
        self.assertTableResultEquals(
            mldb.query("select mime_type(fetcher('mldb/testing/logo-new.jpg')[content]) as mime"),
            [
                [
                    "_rowName",
                    "mime"
                ],
                [
                    "result",
                    "JPEG image data, JFIF standard 1.01"
                ]
            ]
        )
        
        self.assertTableResultEquals(
            mldb.query("select mime_type(fetcher('mldb/testing/MLDB-2166_mime_type.py')[content]) as mime"),
            [
                [
                    "_rowName",
                    "mime"
                ],
                [
                    "result",
                    "Python script, ASCII text executable"
                ]
            ]
        )
    
    def test_not_blob(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                'Mime type extraction requires that an atomic value of type BLOB'):
            rez = mldb.query("""
                select mime_type(fetcher('mldb/testing/logo-new.jpg')) as mime
            """)
    
    def test_non_existant(self):
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                'No such file'):
            rez = mldb.query("""
                select mime_type(fetcher('non_existant')) as mime
            """)

if __name__ == '__main__':
    mldb.run_tests()
