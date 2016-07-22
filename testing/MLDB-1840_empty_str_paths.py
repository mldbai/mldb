#
# MDLB-1840_empty_str_paths.py
# Francois Maillet, 2016-07-21
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class MDLB1840EmptyStrPaths(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(cls):
        pass

    def test_it(self):
        self.assertTableResultEquals(
            mldb.query("""
                select parse_json('{"": 5, "pwet":10}') as *
            """),
            [
                ["_rowName", "", "pwet"],
                [  "result",  5, 10]
            ]
        )

        self.assertTableResultEquals(
            mldb.query("""
                select parse_json('{"": 5, "pwet":10}') as *
                where "" != 5
            """),
            [
                ["_rowName"]
            ]
        )
        
        self.assertTableResultEquals(
            mldb.query("""
                select * from (
                    select parse_json('{"": 5, "pwet":10}') as *
                )
            """),
            [
                ["_rowName", "", "pwet"]
                [  "result",  5, 10]
            ]
        )
        
        self.assertTableResultEquals(
            mldb.query("""
                select pwet from (
                    select parse_json('{"": 5, "pwet":10}') as *
                )
            """),
            [
                ["_rowName", "pwet"]
                [  "result", 10]
            ]
        )
        
        self.assertTableResultEquals(
            mldb.query("""
                select "" from (
                    select parse_json('{"": 5, "pwet":10}') as *
                )
            """),
            [
                ["_rowName", ""]
                [  "result", 5]
            ]
        )



if __name__ == '__main__':
    mldb.run_tests()
