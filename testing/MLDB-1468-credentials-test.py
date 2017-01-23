#
# MLDB-1468-credentials-test.py
# 2016-04-14
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest
from os.path import isfile, join, expanduser

mldb = mldb_wrapper.wrap(mldb) # noqa

class CredentialTest(MldbUnitTest):

    def test_creation_of_dummy_creds(self):
        # try something that should work
        # mldb.get asserts the result status_code is >= 200 and < 400
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                  "doesn't exist"):
            mldb.get("/v1/credentials/s3cred")

        resp = mldb.put("/v1/credentials/s3cred", {
            "store" : {
                "resourceType" : "aws:s3",
                "resource" : "s3://",
                "credential" : {
                    "provider" : "Credentials collection",
                    "protocol" : "http",
                    "location" : "s3.amazonaws.com",
                    "id" : "this is my key",
                    "secret" : "this is my secret"
                }
            }
        })

        mldb.log(resp)

        mldb.get("/v1/credentials/s3cred")

        resp = mldb.delete("/v1/credentials/s3cred")

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                  "doesn't exist"):
            mldb.get("/v1/credentials/s3cred")

        resp = mldb.post("/v1/credentials", {
            "store" : {
                "resourceType" : "aws:s3",
                "resource" : "s3://",
                "credential" : {
                    "provider" : "Credentials collection",
                    "protocol" : "http",
                    "location" : "s3.amazonaws.com",
                    "id" : "this is my key",
                    "secret" : "this is my secret"
                }
            }
        })

        mldb.log(resp)

    def test_selection_of_creds(self):
        # store a dummy credential for a specific path
        resp = mldb.put("/v1/credentials/badcred", {
            "store" : {
                "resourceType" : "aws:s3",
                "resource" : "s3://dummy",
                "credential" : {
                    "provider" : "Credentials collection",
                    "protocol" : "http",
                    "location" : "s3.amazonaws.com",
                    "id" : "this is my key",
                    "secret" : "this is my secret"
                }
            }
        })

        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 's3://dummy/test.csv',
                "outputDataset": {
                    "id": "test"
                },
                "runOnCreation": True
            }
        }

        # this is expected to pick the most specific but invalid credentials
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            resp = mldb.put("/v1/procedures/import", csv_conf)


    @unittest.skipIf(not isfile(join(expanduser('~'), '.cloud_credentials')),
                     "skipping because no credentials were found")
    def test_selection_of_creds_2(self):

        with open(join(expanduser('~'), '.cloud_credentials')) as f:
            creds = f.readlines()
            for idx, cred in enumerate(creds):
                type, version, key, secret = cred.split('\t')

                # store the credential for a specific path
                resp = mldb.put("/v1/credentials/testcred" + str(idx), {
                    "store" : {
                        "resourceType" : "aws:s3",
                        "resource" : "s3://private-mldb-ai/testing",
                        "credential" : {
                            "provider" : "Credential collections",
                            "protocol" : "http",
                            "location" : "s3.amazonaws.com",
                            "id" : key,
                            "secret" : secret.strip()
                        }
                    }
                })

        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 's3://private-mldb-ai/testing/MLDB-1468/test.csv',
                "outputDataset": {
                    "id": "test"
                },
                "runOnCreation": True
            }
        }

        # this is expecting to pick key for path s3://private-mldb-ai/testing
        mldb.put("/v1/procedures/import", csv_conf)

        # store the credential for a specific path
        resp = mldb.put("/v1/credentials/testcredwrong", {
            "store" : {
                "resourceType" : "aws:s3",
                "resource" : "s3://private-mldb-ai/testing/MLDB-1468",
                "credential" : {
                    "provider" : "Credential collections",
                    "protocol" : "http",
                    "location" : "s3.amazonaws.com",
                    "id" : "dummy",
                    "secret" : "dummy"
                }
            }
        })

        # this is expected to pick the most specific but invalid credentials
        with self.assertRaises(mldb_wrapper.ResponseException) as re:
            mldb.put("/v1/procedures/import", csv_conf)

    def test_delete(self):
        "MLDB-1468"
        url = '/v1/credentials/test_delete'
        config = {
            "store" : {
                "resourceType" : "aws:s3",
                "resource" : "s3://dev.mldb.datacratic.com/test_delete",
                "credential" : {
                    "provider" : "Credential collections",
                    "protocol" : "http",
                    "location" : "s3.amazonaws.com",
                    "id" : "dummy",
                    "secret" : "dummy"
                }
            }
        }
        mldb.put(url, config)

        msg = "entry 'test_delete' already exists"
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            mldb.put(url, config)

        mldb.delete(url)
        mldb.put(url, config)

mldb.run_tests()
