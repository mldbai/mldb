#
# hash_test.py
# Francois-Michel L Heureux, 2016-07-08
# This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
#

mldb = mldb_wrapper.wrap(mldb)  # noqa

class HashTest(MldbUnitTest):  # noqa

    def test_it(self):
        res = mldb.query("SELECT hash(1)")
        mldb.log(res)

        res = mldb.query("SELECT hash('1')")
        mldb.log(res)

        res = mldb.query("SELECT hash({a: 12, b: 'coco'})")
        mldb.log(res)

        res = mldb.query("SELECT hash(NULL)")
        mldb.log(res)


if __name__ == '__main__':
    mldb.run_tests()
