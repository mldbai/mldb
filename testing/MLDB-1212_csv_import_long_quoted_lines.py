#
# MLDB-1212_csv_import_long_quoted_lines.py
# 2016
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

import unittest

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb1212(MldbUnitTest):
    @classmethod
    def setUpClass(self):
        pass

    def test_long_quoted_lines(self):
        with open("tmp/broken_csv.csv", 'wb') as f:
            f.write("a,b\n")
            f.write("1,\"" + " ".join(["word " for x in xrange(50)])+"\"\n")
            f.write("1,\"" + " ".join(["word " for x in xrange(100)])+"\"\n")
            f.write("1,\"" + " ".join(["word " for x in xrange(1000)])+"\"\n")
            f.write("1,\"" + " ".join(["word " for x in xrange(10000)])+"\"\n")

        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'file://tmp/broken_csv.csv',
                "outputDataset": {
                    "id": "x",
                },
                "runOnCreation": True,
                "ignoreBadLines": False
            }
        }
        mldb.put("/v1/procedures/csv_proc", csv_conf)

        result = mldb.get(
            "/v1/query",
            q="select tokenize(b, {splitChars: ' '}) as cnt "
            "from x order by rowName() ASC")
        js_rez = result.json()
        mldb.log(js_rez)

        answers = {"2": 50, "3": 100, "4": 1000, "5": 10000}
        for row in js_rez:
            self.assertEqual(answers[row["rowName"]], row["columns"][0][1])


    def test_multiline(self):
        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'file://./mldb/testing/dataset/MLDB-1652_multiline.csv',
                "outputDataset": "multiline",
                "runOnCreation": True,
                "allowMultiLines": True,
                "ignoreBadLines": False
            }
        }

        mldb.put("/v1/procedures/csv_proc_multi", csv_conf)

        self.assertTableResultEquals(
            mldb.query("select * from multiline"),
            [
                ["_rowName", "colA", "colB  B B", "colC"],
                [       "2",  "a", "b", "c" ],
                [       "3",  "a", "bouya , hoho", "c2" ],
                [       "4",  "a5", "b5", "c5" ],
            ]
        )

    def test_multiline_limit(self):
        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'file://./mldb/testing/dataset/MLDB-1652_multiline.csv',
                "outputDataset": "multiline",
                "runOnCreation": True,
                "allowMultiLines": True,
                "ignoreBadLines": False,
                "limit": 2
            }
        }

        mldb.put("/v1/procedures/csv_proc_multi", csv_conf)

        self.assertTableResultEquals(
            mldb.query("select * from multiline"),
            [
                ["_rowName", "colA", "colB  B B", "colC"],
                [       "2",  "a", "b", "c" ],
                [       "3",  "a", "bouya , hoho", "c2" ]
            ]
        )
   
    # offset not supported yet
    @unittest.expectedFailure
    def test_multiline_offset(self):
        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : 'file://./mldb/testing/dataset/MLDB-1652_multiline.csv',
                "outputDataset": "multiline",
                "runOnCreation": True,
                "allowMultiLines": True,
                "ignoreBadLines": False,
                "offset": 2,
                "limit": 2
            }
        }

        mldb.put("/v1/procedures/csv_proc_multi", csv_conf) 

        self.assertTableResultEquals(
            mldb.query("select * from multiline"),
            [
                ["_rowName", "colA", "colB  B B", "colC"],
                [       "4",  "a5", "b5", "c5" ],
            ]
        )


mldb.run_tests()

