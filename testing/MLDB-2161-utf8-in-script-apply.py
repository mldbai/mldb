# coding=utf-8 #
# MLDB-2161-utf8-in-script-apply.py
# Guy Dumais, 2017-03-08
# This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB2161Utf8InScriptApply(MldbUnitTest):  # noqa

    def test_python_script_apply_with_utf8(self):
        mldb.put("/v1/functions/filter_top_themes", {
            "type": "script.apply",
            "params": {
                "language": 'python',
                "scriptConfig": {
                    "source": """
# retrieve all themes
mldb.log(mldb.script.args)

mldb.script.set_return([[str(mldb.script.args[0][1]), 0, '1970-01-01T00:00:00.0000000Z']])
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
                   "return.['Th\\xc3\\xa8mes et sous-th\\xc3\\xa0mes', [0, '-Inf']]"
               ],
                [
                    "result",
                    0
                ]
            ]
        )

if __name__ == '__main__':
    mldb.run_tests()
