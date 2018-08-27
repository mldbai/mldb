# coding=utf-8 #
# MLDB-2161-utf8-in-script-apply.py
# Guy Dumais, 2017-03-08
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

from mldb import mldb, MldbUnitTest, ResponseException

class MLDB2161Utf8InScriptApply(MldbUnitTest):  # noqa

    def test_python_script_apply_with_utf8(self):
        mldb.put("/v1/functions/filter_top_themes", {
            "type": "script.apply",
            "params": {
                "language": 'python',
                "scriptConfig": {
                    "source": """
from mldb import mldb
# retrieve all themes
mldb.log(mldb.script.args)

request.set_return([[str(mldb.script.args[0][1]), 0, '1970-01-01T00:00:00.0000000Z']])
"""
                }
            }
        })
        
        self.assertTableResultEquals(mldb.query("""
        SELECT filter_top_themes(
            {{"Politique Provinciale":2, "Élections":1, "Thèmes et sous-thàmes":0} AS args}
        ) AS *
        """),
            [
               [
                   "_rowName",
                   "return.['Thèmes et sous-thàmes', [0, '-Inf']]"
               ],
                [
                    "result",
                    0
                ]
            ]
        )

if __name__ == '__main__':
    request.set_return(mldb.run_tests())
