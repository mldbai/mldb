#
# MLDBFB-399_put_post_creds.py
# Mich, 2016-03-11
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#
import unittest

mldb = mldb_wrapper.wrap(mldb)  # noqa

class TestPutPostCreds(MldbUnitTest):  # noqa

    @unittest.expectedFailure
    def test_put(self):
        mldb.put("/v1/creds/rules/mys3creds", {
            "store": {
                "resource":"s3://",
                "resourceType":"aws:s3",
                "credential": {
                    "protocol": "http",
                    "location":"s3.amazonaws.com",
                    "id": "<ACCESS KEY ID>",
                    "secret": "<ACCESS KEY>",
                    "validUntil":"2030-01-01T00:00:00Z"
                }
            }
        })

    @unittest.expectedFailure
    def test_post(self):
        mldb.post("/v1/creds/rules", {
            "store": {
                "resource":"s3://",
                "resourceType":"aws:s3",
                "credential": {
                    "protocol": "http",
                    "location":"s3.amazonaws.com",
                    "id": "<ACCESS KEY ID>",
                    "secret": "<ACCESS KEY>",
                    "validUntil":"2030-01-01T00:00:00Z"
                }
            }
        })

if __name__ == '__main__':
    mldb.run_tests()
