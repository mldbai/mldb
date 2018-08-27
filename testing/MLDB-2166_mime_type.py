#
# MLDB-2166_mime_type.py
# Francois Maillet, 2017-03-10
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

from mldb import mldb, MldbUnitTest, ResponseException

class MLDB2166MimeType(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        pass

    def test_it(self):
        self.assertTableResultEquals(
            mldb.query("select regex_search(mime_type(fetcher('mldb/testing/logo-new.jpg')[content]), 'JPEG image data') as mime"),
            [
                [
                    "_rowName",
                    "mime"
                ],
                [
                    "result",
                    1
                ]
            ]
        )
        
        # self.assertTableResultEquals(
        #     mldb.query("select mime_type(fetcher('mldb/testing/MLDB-2166_mime_type.py')[content]) as mime"),
        #     [
        #         [
        #             "_rowName",
        #             "mime"
        #         ],
        #         [
        #             "result",
        #             "Python script, ASCII text executable"
        #         ]
        #     ]
        # )
    
    def test_not_blob(self):
        with self.assertRaisesRegex(ResponseException,
                'Mime type extraction requires that an atomic value of type BLOB'):
            rez = mldb.query("""
                select mime_type(fetcher('mldb/testing/logo-new.jpg')) as mime
            """)
    
    def test_non_existant(self):
        with self.assertRaisesRegex(ResponseException,
                'No such file'):
            rez = mldb.query("""
                select mime_type(fetcher('non_existant')) as mime
            """)

if __name__ == '__main__':
    request.set_return(mldb.run_tests())
