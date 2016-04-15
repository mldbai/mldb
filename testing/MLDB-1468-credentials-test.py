#
# MLDB-1468-credentials-test.py
# 2016-04-14
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class CredentialTest(MldbUnitTest):

    def test_creation_of_volatile_creds(self):
        # try something that should work
        # mldb.get asserts the result status_code is >= 200 and < 400
        resp = mldb.put("/v1/credentials/s3cred", {
            "store" : {
                "resourceType" : "aws:s3",
                "resource" : "s3://",
                "role" : "",
                "operation" : "",
                "expiration" : "",
                "credential" : {
                    "provider" : "",
                    "protocol" : "http",
                    "location" : "s3.amazonaws.com",
                    "id" : "this is my key",
                    "secret" : "this is my secret",
                    "validUntil" : "2016-01-01"
                }
            }
        })

        mldb.log(resp)

mldb.run_tests()
