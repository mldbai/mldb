#
# try_except_builtin_fct.py
# Francois-Michel L Heureux, 2016-07-11
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb)  # noqa

class TryExceptBuiltinFct(MldbUnitTest):  # noqa

    def test_it(self):
        res = mldb.query("""
            SELECT parse_json(5)
        """)
        self.assertEqual(res[1][1], 5)

        msg = 'Executing builtin function parse_json'
        with self.assertRaisesRegexp(mldb_wrapper.ResponseException, msg):
            res = mldb.query("""
                SELECT parse_json('coco')
            """)

        res = mldb.query("""
            SELECT try(parse_json(5), 'err')
        """)
        self.assertEqual(res[1][1], 5)

        res = mldb.query("""
            SELECT try(parse_json('coco'), 'err')
        """)
        self.assertEqual(res[1][1], 'err')



if __name__ == '__main__':
    mldb.run_tests()
