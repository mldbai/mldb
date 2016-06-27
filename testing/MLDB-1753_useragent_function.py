#
# MLDB-1753_useragent_function.py
# Francois Maillet, 2016-06-27
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1753UseragentFunction(MldbUnitTest):  # noqa
    @classmethod
    def setUpClass(cls):
        mldb.put("/v1/functions/useragent", {
                "type": "parse.useragent",
                "params": {
                        "regexFile": "mldb/ext/uap-core/regexes.yaml"
                    }
            })

    def test_parsing_works(self):
        self.assertTableResultEquals(
            mldb.query("select useragent({ua: 'Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3'}) as *"),
            [
                [
                    "_rowName",
                    "browser.family",
                    "browser.version",
                    "device.brand",
                    "device.model",
                    "isSpider",
                    "os.family",
                    "os.version"
                ],
                [
                    "result",
                    "Mobile Safari",
                    "5.1.0",
                    "Apple",
                    "iPhone",
                    0,
                    "iOS",
                    "5.1.1"
                ]
            ]
        )


if __name__ == '__main__':
    mldb.run_tests()
