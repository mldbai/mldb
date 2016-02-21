#
# MLDB-1402_twitter_importer.py
# Francois Maillet, 20 fevrier 2016
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

import unittest
import json, os

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1402Test(MldbUnitTest):  

    @classmethod
    def setUpClass(self):
        pass
        
    def test_search(self):

        creds = json.loads("".join(open(os.getenv("HOME") + "/.twitter_credentials.json").readlines()))

        rez = mldb.put("/v1/procedures/twitter", {
            "type": "import.twitter",
            "params": {
                "consumerKey": creds["consumerKey"],
                "consumerSecret": creds["consumerSecret"],
                "username": creds["username"],
                "password": creds["password"],
                "searchQuery": "#mchacks",
                "resultCount": 750,
                "runOnCreation": True,
                "outputDataset": "myTweet"
            }
        })
        mldb.log(rez.json())

        # try something that should work
        # mldb.get asserts the result status_code is >= 200 and < 400
        mldb.log(mldb.get("/v1/query", q="select * from myTweet limit 2"))


mldb.run_tests()
