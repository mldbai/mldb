#
# MLDB-1753_useragent_function.py
# Francois Maillet, 2016-06-27
# This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MLDB1753UseragentFunction(MldbUnitTest):  # noqa
    @classmethod
    def setUpClass(cls):
        mldb.put("/v1/functions/useragent", {
                "type": "http.useragent",
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

    # MLDB-1772
    def test_domain_parsing(self):
        self.assertTableResultEquals(
            mldb.query("""
                select 
                    extract_domain('http://www.datacratic.com/pwetpwet/houa.html') as c1,
                    extract_domain('http://datacratic.com/pwetpwet/houa.html') as c2,
                    extract_domain('http://data.datacratic.com/pwetpwet/houa.html') as c3,

                    extract_domain('http://www.datacratic.com/pwetpwet/houa.html', {removeSubdomain:1}) as c1nosub,
                    extract_domain('http://datacratic.com/pwetpwet/houa.html', {removeSubdomain:1}) as c2nosub,
                    extract_domain('http://data.datacratic.com/pwetpwet/houa.html', {removeSubdomain:1}) as c3nosub
            """),
                [
                    [
                        "_rowName",
                        "c1",
                        "c1nosub",
                        "c2",
                        "c2nosub",
                        "c3",
                        "c3nosub"
                    ],
                    [
                        "result",
                        "www.datacratic.com",
                        "datacratic.com",
                        "datacratic.com",
                        "datacratic.com",
                        "data.datacratic.com",
                        "datacratic.com"
                    ]
                ])

        with self.assertRaisesRegexp(mldb_wrapper.ResponseException,
                                  'Attempt to create a URL without a scheme'):
                mldb.query("SELECT extract_domain('pwet.com') as c4")


        self.assertTableResultEquals(
            mldb.query("""
                select extract_domain(patate) as domain,
                       extract_domain(value) as domain2
                from (
                    select * from row_dataset({"domain": 'http://www.domain.com'})
                )
            """),
                [
                    ["_rowName", "domain", "domain2"],
                    ["0", None, "www.domain.com"]
                ])

    def test_null_input(self):
        self.assertTableResultEquals(
            mldb.query("select useragent({ua: NULL}) as *"),
            [["_rowName","browser.family","browser.version","device.brand","device.model","isSpider","os.family","os.version"],
            ["result",None,None,None,None,None,None,None]])


if __name__ == '__main__':
    mldb.run_tests()
